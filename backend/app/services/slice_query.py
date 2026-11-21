"""切片查询领域服务：多维立方消费，按维度切片、过滤和排序；切片复现文件归档。"""
from __future__ import annotations

import datetime
from typing import Any

from app.analytics_engine import (
    compare_slice_coverage,
    match_slice_filter_condition,
    parse_slice_filter_expr,
    run_slice_with_lineage,
    slice_input_fingerprint,
    slice_template_fingerprint,
    summarize_slice_verdicts,
)
from app.services.base import BaseModuleService
from app.store import store

# 切片复现档案存放的内部文档集合（不作为业务模块暴露）
ARCHIVE_COLLECTION = "slice_archive"

# 档案状态
ARCHIVE_OK = "已归档"
ARCHIVE_FAILED = "写入失败"

# 模板快照要保全的取数条件字段
TEMPLATE_SNAPSHOT_FIELDS = (
    "模板名称", "数据立方", "查询维值", "过滤条件", "排序规则", "返回行数", "缓存TTL",
)


class SliceQueryService(BaseModuleService):
    MODULE = "slice_query"
    ENTITY = "查询模板"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["模板名称", "数据立方", "查询维值", "过滤条件", "排序规则", "返回行数", "缓存TTL"]
    REQUIRED_FIELDS = ["模板名称", "数据立方", "查询维值"]
    STATUS_ORDER = ["草稿", "已保存", "已发布", "已过期"]
    ACTION_RULES = {"保存模板": "已保存", "发布模板": "已发布", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = ["标记过期"]

    # ---- 业务方法 ----

    def execute_slice_query(
        self, template_id: int, data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """执行切片查询：根据模板配置对数据筛选、排序和截断。"""
        template = store.find(self.MODULE, template_id)
        if template is None:
            return {"ok": False, "message": f"查询模板 {template_id} 不存在"}

        run = run_slice_with_lineage(template, data)
        return {
            "ok": True,
            "template_name": str(template.get("模板名称", "")),
            "result_rows": len(run["result_rows"]),
            "total_rows": run["total_rows"],
            "row_limit": run["row_limit"],
            "cache_ttl": _parse_int(template.get("缓存TTL"), 300),
            "data": run["result_rows"],
        }

    def get_templates_for_cube(self, cube_name: str) -> list[dict[str, Any]]:
        """获取某立方关联的所有查询模板（供即席导出消费）。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("数据立方") == cube_name and row.get("status") == "已发布"
        ]

    # ---- 切片复现文件归档 ----

    def create_archive(
        self,
        template_id: int,
        data: list[dict[str, Any]],
        report_task_id: int | None = None,
        simulate_write_failure: bool = False,
    ) -> dict[str, Any]:
        """按切片模板×输入数据归档切片复现文件。

        保全当时的取数条件（模板快照）和原始行序，逐行记录命中/筛掉判定，
        原明细分区退役后仍可复查。simulate_write_failure 用于模拟档案写入
        失败：捕获的判定结果留在暂存区，等待 retry_archive 重试落盘。
        """
        template = store.find(self.MODULE, template_id)
        if template is None:
            return {"ok": False, "message": f"查询模板 {template_id} 不存在"}
        if not data:
            return {"ok": False, "message": "缺少输入数据，无法制作切片复现档案"}

        run = run_slice_with_lineage(template, data)
        stats = summarize_slice_verdicts(run["verdicts"])
        template_fp = slice_template_fingerprint(template)
        input_fp = slice_input_fingerprint(data)

        archives = store.docs(ARCHIVE_COLLECTION)
        archive_id = max((int(a.get("id", 0)) for a in archives), default=0) + 1
        archive: dict[str, Any] = {
            "id": archive_id,
            "archive_key": f"tpl{template_id}:{template_fp}:{input_fp}",
            "status": ARCHIVE_OK,
            "created_at": datetime.datetime.now(
                datetime.timezone.utc,
            ).isoformat(timespec="seconds"),
            "template_id": template_id,
            "template_snapshot": {
                **{key: str(template.get(key, "") or "") for key in TEMPLATE_SNAPSHOT_FIELDS},
                "模板指纹": template_fp,
            },
            "input_fingerprint": input_fp,
            "stats": stats,
            "rows": run["verdicts"],
            "report_refs": [],
            "write_attempts": 1,
            "archive_basis": "源数据实时计算",
            "review_scope": _full_review_scope(stats),
            "pending_payload": None,
        }

        if simulate_write_failure:
            # 写入失败：首次捕获的判定结果留在暂存区，档案本身不落明细
            archive["status"] = ARCHIVE_FAILED
            archive["pending_payload"] = {"rows": run["verdicts"], "stats": stats}
            archive["rows"] = []
            archive["review_scope"] = {
                "取数条件": "可复核（模板快照已保全）",
                "命中明细": "暂不可复核（档案写入失败，待重试）",
                "筛掉明细": "暂不可复核（档案写入失败，待重试）",
                "源分区": "未知（重试时若已清退，将以首次捕获快照为准）",
            }

        archives.append(archive)

        link_result = None
        if report_task_id is not None:
            link_result = self.link_report(archive_id, report_task_id)

        return {
            "ok": True,
            "archive_id": archive_id,
            "archive_key": archive["archive_key"],
            "status": archive["status"],
            "stats": stats,
            "report_link": link_result,
            "message": (
                "切片复现档案已归档" if archive["status"] == ARCHIVE_OK
                else "档案写入失败，首次捕获快照已暂存，可重试"
            ),
        }

    def retry_archive(
        self, archive_id: int, data: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """档案写入失败后的重试。

        重试时数据库或原分区可能已清退：
        - data 为 None（分区已清退）：以首次捕获快照落盘，标明可复核范围；
        - data 指纹与首次一致（源数据在线）：按保全的取数条件重跑校验后落盘；
        - data 指纹不一致（部分清退/变更）：仍以首次捕获快照为准，说清范围。
        """
        archive = self._find_archive(archive_id)
        if archive is None:
            return {"ok": False, "message": f"切片复现档案 {archive_id} 不存在"}
        if archive["status"] != ARCHIVE_FAILED:
            return {
                "ok": False,
                "message": f"档案 {archive_id} 状态为「{archive['status']}」，无需重试",
            }
        pending = archive.get("pending_payload")
        if not pending:
            return {"ok": False, "message": "首次捕获快照缺失，无法重试"}

        archive["write_attempts"] = int(archive.get("write_attempts", 1)) + 1
        stats: dict[str, int] = pending["stats"]

        if data is None:
            archive["rows"] = pending["rows"]
            archive["stats"] = stats
            archive["archive_basis"] = "首次捕获快照（源分区已清退）"
            archive["review_scope"] = {
                "取数条件": "可复核（模板快照已保全）",
                "命中明细": f"可复核（{stats['kept_rows']} 行，源自首次捕获快照，含原始行序）",
                "筛掉明细": f"可复核（{stats['dropped_rows']} 行，源自首次捕获快照，含原始行序与筛掉原因）",
                "源分区": "已清退，无法重跑校验，明细以首次捕获快照为准",
            }
        elif slice_input_fingerprint(data) == archive["input_fingerprint"]:
            rerun = run_slice_with_lineage(archive["template_snapshot"], data)
            archive["rows"] = rerun["verdicts"]
            archive["stats"] = summarize_slice_verdicts(rerun["verdicts"])
            archive["archive_basis"] = "源数据重跑校验"
            archive["review_scope"] = _full_review_scope(archive["stats"])
        else:
            archive["rows"] = pending["rows"]
            archive["stats"] = stats
            archive["archive_basis"] = "首次捕获快照（源数据已变更）"
            archive["review_scope"] = {
                "取数条件": "可复核（模板快照已保全）",
                "命中明细": f"可复核（{stats['kept_rows']} 行，源自首次捕获快照）",
                "筛掉明细": f"可复核（{stats['dropped_rows']} 行，源自首次捕获快照）",
                "源分区": "已部分清退或变更（输入指纹不一致），变更部分不可重跑校验，明细以首次捕获快照为准",
            }

        archive["status"] = ARCHIVE_OK
        archive["pending_payload"] = None
        return {
            "ok": True,
            "archive_id": archive_id,
            "status": archive["status"],
            "archive_basis": archive["archive_basis"],
            "review_scope": archive["review_scope"],
            "message": "档案重试写入成功",
        }

    def get_archive(self, archive_id: int) -> dict[str, Any]:
        """查询切片复现档案：取数条件、统计行和逐行明细（含被筛掉的行）。"""
        archive = self._find_archive(archive_id)
        if archive is None:
            return {"ok": False, "message": f"切片复现档案 {archive_id} 不存在"}
        return {"ok": True, "archive": archive}

    def list_archives(self) -> dict[str, Any]:
        """列出全部切片复现档案（摘要，不含逐行明细）。"""
        summaries = [
            {
                key: archive.get(key)
                for key in (
                    "id", "archive_key", "status", "created_at",
                    "template_id", "stats", "report_refs", "archive_basis",
                )
            }
            for archive in store.docs(ARCHIVE_COLLECTION)
        ]
        return {"ok": True, "total": len(summaries), "items": summaries}

    def compare_archive_with_current(
        self, archive_id: int, data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """档案明细 vs 现行切片：用现行（可能已修订的）模板跑当前数据并对比覆盖。"""
        archive = self._find_archive(archive_id)
        if archive is None:
            return {"ok": False, "message": f"切片复现档案 {archive_id} 不存在"}
        if archive["status"] != ARCHIVE_OK:
            return {"ok": False, "message": f"档案 {archive_id} 尚未归档成功，无法对比"}
        if not data:
            return {"ok": False, "message": "缺少当前输入数据，无法执行现行切片"}
        template = store.find(self.MODULE, int(archive["template_id"]))
        if template is None:
            return {"ok": False, "message": "现行模板已删除，无法执行现行切片"}

        current_run = run_slice_with_lineage(template, data)
        coverage = compare_slice_coverage(archive["rows"], current_run["verdicts"])
        current_fp = slice_template_fingerprint(template)
        return {
            "ok": True,
            "archive_id": archive_id,
            "archive_condition": archive["template_snapshot"],
            "current_condition": {
                **{key: str(template.get(key, "") or "") for key in TEMPLATE_SNAPSHOT_FIELDS},
                "模板指纹": current_fp,
            },
            "template_revised": current_fp != archive["template_snapshot"]["模板指纹"],
            "coverage": coverage,
            "summary": {
                "档案命中": len(coverage["archive_kept"]),
                "档案筛掉": len(coverage["archive_dropped"]),
                "现行命中": len(coverage["current_kept"]),
                "现行筛掉": len(coverage["current_dropped"]),
                "现行新增覆盖": len(coverage["newly_covered_by_current"]),
                "仅档案命中": len(coverage["only_kept_in_archive"]),
            },
        }

    def revise_template(
        self, template_id: int, changes: dict[str, Any],
    ) -> dict[str, Any]:
        """修订模板取数条件（如放宽过滤条件）。

        已归档的切片复现文件不受影响：档案里的模板快照仍是当时版本，
        修订前后指纹一并返回，便于审计确认「模板确实被改过」。
        """
        template = store.find(self.MODULE, template_id)
        if template is None:
            return {"ok": False, "message": f"查询模板 {template_id} 不存在"}
        editable = [key for key in changes if key in TEMPLATE_SNAPSHOT_FIELDS]
        if not editable:
            return {
                "ok": False,
                "message": f"没有可修订的取数条件字段（支持：{'、'.join(TEMPLATE_SNAPSHOT_FIELDS)}）",
            }
        before_fp = slice_template_fingerprint(template)
        before = {key: str(template.get(key, "") or "") for key in editable}
        for key in editable:
            template[key] = str(changes[key])
        after_fp = slice_template_fingerprint(template)
        return {
            "ok": True,
            "template_id": template_id,
            "revised_fields": editable,
            "before": before,
            "after": {key: str(template.get(key, "") or "") for key in editable},
            "模板指纹_修订前": before_fp,
            "模板指纹_修订后": after_fp,
            "message": f"模板 {template_id} 已修订：{'、'.join(editable)}",
        }

    def link_report(self, archive_id: int, task_id: int) -> dict[str, Any]:
        """把切片复现档案关联到即席导出任务（当期报表）。"""
        archive = self._find_archive(archive_id)
        if archive is None:
            return {"ok": False, "message": f"切片复现档案 {archive_id} 不存在"}
        task = store.find("adhoc_export", task_id)
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}
        task["关联档案"] = archive_id
        refs = archive.setdefault("report_refs", [])
        if not any(int(ref.get("task_id", 0)) == int(task_id) for ref in refs):
            refs.append({
                "module": "adhoc_export",
                "task_id": task_id,
                "任务名称": str(task.get("任务名称", "")),
            })
        return {
            "ok": True,
            "archive_id": archive_id,
            "task_id": task_id,
            "message": f"档案 {archive_id} 已关联到导出任务 {task_id}",
        }

    def _find_archive(self, archive_id: int) -> dict[str, Any] | None:
        return store.find_doc(ARCHIVE_COLLECTION, archive_id)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "查询模板数据集"
        base["published_templates"] = sum(
            1 for row in store.rows(self.MODULE) if row.get("status") == "已发布"
        )
        base["archived_slices"] = sum(
            1 for doc in store.docs(ARCHIVE_COLLECTION) if doc.get("status") == ARCHIVE_OK
        )
        return base


def _full_review_scope(stats: dict[str, int]) -> dict[str, str]:
    """源数据在线时的可复核范围说明。"""
    return {
        "取数条件": "可复核（模板快照已保全）",
        "命中明细": f"可复核（{stats['kept_rows']} 行，含原始行序）",
        "筛掉明细": f"可复核（{stats['dropped_rows']} 行，含原始行序与筛掉原因）",
        "源分区": "可重跑校验（归档时源数据在线）",
    }


def _parse_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _apply_slice_filter(
    rows: list[dict[str, Any]], filter_expr: str,
) -> list[dict[str, Any]]:
    """解析过滤表达式并应用。支持 field>value; field<value 等。"""
    results = list(rows)
    for cond in parse_slice_filter_expr(filter_expr):
        results = [
            row for row in results
            if match_slice_filter_condition(row, cond["field"], cond["op"], cond["value"])
        ]
    return results

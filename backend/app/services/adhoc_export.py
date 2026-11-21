"""即席导出领域服务：异步任务调度、文件生成、格式转换。"""
from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from typing import Any

from app.services.base import BaseModuleService
from app.services.slice_query import ARCHIVE_COLLECTION
from app.store import store
from app.analytics_engine import ExportScheduler


class AdhocExportService(BaseModuleService):
    MODULE = "adhoc_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["任务名称", "查询SQL", "导出格式", "行数限制", "是否压缩", "分区过滤", "创建时间"]
    REQUIRED_FIELDS = ["任务名称", "查询SQL", "导出格式"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "导出失败", "已过期"]
    ACTION_RULES = {"开始导出": "执行中", "完成导出": "已完成", "标记失败": "导出失败", "清理过期": "已过期"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def __init__(self) -> None:
        super().__init__()
        self._scheduler = ExportScheduler(max_concurrent=3)

    # ---- 业务方法 ----

    def submit_export(
        self, task_id: int, query_data: list[dict[str, Any]],
        archive_id: int | None = None,
    ) -> dict[str, Any]:
        """提交导出任务并异步执行；可同时把切片复现档案关联到当期报表。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}

        export_format = str(task.get("导出格式", "csv")).lower()
        row_limit = _parse_int(task.get("行数限制"), 1000)
        compressed = str(task.get("是否压缩", "否")).lower() in ("是", "true", "yes", "1")

        self._scheduler.enqueue(
            task_id=str(task_id),
            priority=1,
            sql=str(task.get("查询SQL", "")),
            format=export_format,
        )

        self.run_action(task_id, "开始导出")

        # 同步执行导出（简化版）
        try:
            file_content = _generate_export_file(
                query_data[:row_limit], export_format, compressed,
            )
            self.run_action(task_id, "完成导出")
            result: dict[str, Any] = {
                "ok": True,
                "task_id": task_id,
                "task_name": str(task.get("任务名称", "")),
                "format": export_format,
                "row_count": min(len(query_data), row_limit),
                "compressed": compressed,
                "file_size_bytes": len(file_content) if isinstance(file_content, bytes) else len(file_content.encode()),
                "content_preview": (
                    file_content[:500] if isinstance(file_content, str)
                    else file_content[:500].decode("utf-8", errors="replace")
                ),
            }
            if archive_id is not None:
                result["archive_link"] = self.link_archive(task_id, archive_id)
            return result
        except Exception as exc:
            self.run_action(task_id, "标记失败")
            return {"ok": False, "message": f"导出失败: {exc}"}

    def link_archive(self, task_id: int, archive_id: int) -> dict[str, Any]:
        """把切片复现档案关联到导出任务，旧报表汇总由此可回溯明细。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}
        archive = store.find_doc(ARCHIVE_COLLECTION, archive_id)
        if archive is None:
            return {"ok": False, "message": f"切片复现档案 {archive_id} 不存在"}
        task["关联档案"] = archive_id
        refs = archive.setdefault("report_refs", [])
        if not any(int(ref.get("task_id", 0)) == int(task_id) for ref in refs):
            refs.append({
                "module": self.MODULE,
                "task_id": task_id,
                "任务名称": str(task.get("任务名称", "")),
            })
        return {
            "ok": True,
            "task_id": task_id,
            "archive_id": archive_id,
            "message": f"切片复现档案 {archive_id} 已关联到导出任务 {task_id}",
        }

    def get_export_lineage(self, task_id: int) -> dict[str, Any]:
        """报表回溯：旧报表汇总 → 实际参与计算的交易明细与真实取数条件。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}
        archive_id = task.get("关联档案")
        if archive_id is None:
            return {"ok": False, "message": f"导出任务 {task_id} 未关联切片复现档案"}
        archive = store.find_doc(ARCHIVE_COLLECTION, int(archive_id))
        if archive is None:
            return {"ok": False, "message": f"关联的切片复现档案 {archive_id} 不存在"}
        rows = archive.get("rows", [])
        return {
            "ok": True,
            "task": {
                "id": task.get("id"),
                "任务名称": str(task.get("任务名称", "")),
                "status": str(task.get("status", "")),
                "创建时间": str(task.get("创建时间", "")),
            },
            "archive": {
                "id": archive.get("id"),
                "archive_key": archive.get("archive_key"),
                "status": archive.get("status"),
                "archive_basis": archive.get("archive_basis"),
                "取数条件": archive.get("template_snapshot"),
                "统计行": archive.get("stats"),
                "实际参与计算的明细": [v for v in rows if v.get("kept")],
                "当时筛掉的明细": [v for v in rows if not v.get("kept")],
                "复核范围": archive.get("review_scope"),
            },
        }

    def batch_export(
        self, task_ids: list[int], query_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """批量提交导出任务。"""
        results: list[dict[str, Any]] = []
        for tid in task_ids:
            result = self.submit_export(tid, query_data)
            results.append(result)
        success = sum(1 for r in results if r.get("ok"))
        return {
            "ok": True,
            "total_tasks": len(task_ids),
            "success_count": success,
            "fail_count": len(task_ids) - success,
            "results": results,
        }

    def get_queue_status(self) -> dict[str, Any]:
        """获取导出队列状态。"""
        return {
            "ok": True,
            "queue_size": self._scheduler.queue_size(),
            "running_count": self._scheduler.running_count(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "导出任务数据集"
        base["queue_size"] = self._scheduler.queue_size()
        return base


def _parse_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _generate_export_file(
    data: list[dict[str, Any]], format: str, compressed: bool,
) -> str | bytes:
    """生成导出文件内容。"""
    if not data:
        return "" if format == "csv" else "[]"

    if format == "csv":
        output = io.StringIO()
        output.write("\ufeff")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(list(data[0].keys()))
        for row in data:
            writer.writerow(list(row.values()))
        return output.getvalue()

    elif format in ("json", "jsonl"):
        if format == "jsonl":
            return "\n".join(json.dumps(row) for row in data)
        return json.dumps(data, ensure_ascii=False, indent=2)

    elif format == "parquet":
        # 简化parquet: JSON序列化
        return json.dumps(data, ensure_ascii=False).encode()

    return json.dumps(data)
"""切片复现档案端到端演示：两笔位于旧模板门槛两侧的订单。

场景复现需求全链路：
1. 旧模板「大额订单切片」过滤条件 amount>=500（门槛 500）
2. 取两笔位于门槛两侧的订单制作存档：A=600（命中）、B=400（被筛掉）
3. 即席导出把切片复现档案关联到当期报表
4. 演示档案写入失败 → 原分区清退 → 重试时的可复核范围说明
5. 模板修订：过滤条件放宽为 amount>=300
6. 对比档案明细和现行切片分别覆盖哪几笔
7. 旧报表汇总回溯实际参与计算的交易明细

运行：cd backend && .venv/bin/python scripts/demo_slice_archive.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402

slice_svc = service_for("slice_query")
export_svc = service_for("adhoc_export")


def show(title: str, payload: object) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    # ---- 步骤 1：登记旧模板（门槛 amount>=500）并发布 ----
    template, missing = slice_svc.create_entry({
        "模板名称": "大额订单切片",
        "数据立方": "交易分析立方",
        "查询维值": "渠道=线上",
        "过滤条件": "amount>=500",
        "排序规则": "amount:DESC",
        "返回行数": "100",
        "缓存TTL": "300",
    })
    assert template is not None, f"模板登记失败：{missing}"
    tpl_id = int(template["id"])
    slice_svc.run_action(tpl_id, "保存模板")
    slice_svc.run_action(tpl_id, "发布模板")
    show("步骤1：旧模板（过滤条件 amount>=500，门槛 500）", store.find("slice_query", tpl_id))

    # ---- 步骤 2：两笔位于旧模板门槛两侧的订单 ----
    orders = [
        {"order_id": "A1001", "user_id": "u01", "amount": 600, "city": "上海", "渠道": "线上"},
        {"order_id": "B2002", "user_id": "u02", "amount": 400, "city": "北京", "渠道": "线上"},
    ]
    show("步骤2：两笔位于门槛两侧的订单（A=600 上侧，B=400 下侧）", orders)

    # ---- 步骤 3：登记当期报表（即席导出任务）并关联切片复现档案 ----
    task, missing = export_svc.create_entry({
        "任务名称": "11月大额订单周报",
        "查询SQL": "SELECT order_id, amount FROM dwd_trade_fact WHERE dt='2026-11-20'",
        "导出格式": "csv",
        "行数限制": "1000",
        "是否压缩": "否",
        "分区过滤": "dt='2026-11-20'",
        "创建时间": "2026-11-20",
    })
    assert task is not None, f"导出任务登记失败：{missing}"
    task_id = int(task["id"])

    archive_result = slice_svc.create_archive(tpl_id, orders, report_task_id=task_id)
    archive_id = int(archive_result["archive_id"])
    show("步骤3：制作切片复现档案并关联到当期报表", archive_result)

    # 即席导出提交时也支持关联（幂等，不会重复挂接）
    submit_result = export_svc.submit_export(task_id, orders, archive_id=archive_id)
    show("步骤3b：即席导出提交（关联档案到当期报表）", {
        "ok": submit_result["ok"], "task_id": submit_result["task_id"],
        "archive_link": submit_result.get("archive_link"),
    })

    # ---- 步骤 4：查档案明细（原明细退役后复查的依据）----
    archive_view = slice_svc.get_archive(archive_id)
    show("步骤4：档案明细（真实取数条件 + 统计行 + 原始行序 + 筛掉原因）", archive_view)

    # ---- 步骤 5：写入失败 → 分区清退 → 重试 ----
    failed = slice_svc.create_archive(tpl_id, orders, simulate_write_failure=True)
    failed_id = int(failed["archive_id"])
    show("步骤5a：档案写入失败（首次捕获快照已暂存）", failed)
    # 模拟原事实分区已清退：重试时不再提供源数据
    retried = slice_svc.retry_archive(failed_id, data=None)
    show("步骤5b：分区清退后重试（说清可复核范围）", retried)

    # ---- 步骤 6：模板修订，过滤条件放宽为 amount>=300 ----
    revised = slice_svc.revise_template(tpl_id, {"过滤条件": "amount>=300"})
    show("步骤6：模板被修订（过滤条件放宽为 amount>=300）", revised)

    # ---- 步骤 7：放宽后对比——档案明细 vs 现行切片分别覆盖哪几笔 ----
    compare = slice_svc.compare_archive_with_current(archive_id, orders)
    show("步骤7：档案明细 vs 现行切片覆盖对比", compare)

    # ---- 步骤 8：旧报表汇总回溯实际参与计算的交易明细 ----
    lineage = export_svc.get_export_lineage(task_id)
    show("步骤8：旧报表回溯（汇总 → 实际参与计算的交易明细）", lineage)

    print("\n演示完成：档案覆盖 1 笔命中（A1001）+ 1 笔当时筛掉（B2002）；")
    print("放宽过滤后现行切片覆盖 2 笔（A1001、B2002），差额即审计要核对的当时筛掉行。")


if __name__ == "__main__":
    main()

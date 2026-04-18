"""VJudge xlsx ranking import helpers.

This module intentionally has no bot API or database dependencies. It parses
the first sheet of a VJudge-style xlsx file into structured ranking rows.
"""

from __future__ import annotations

import posixpath
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from xml.etree import ElementTree


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
RANK_HEADERS = {"rank", "排名"}
TEAM_HEADERS = {"team", "user", "用户"}
MAX_HEADER_SCAN_ROWS = 20


class VJudgeImportError(Exception):
    """Raised when the imported xlsx cannot be parsed as a valid ranking."""


@dataclass(frozen=True)
class VJudgeStandingRow:
    """One parsed standing row from a VJudge xlsx export."""

    row_no: int
    rank: int
    team_raw: str
    candidate_nickname: str


def extract_candidate_nickname(team_raw: str) -> str:
    """Extract the qrating nickname candidate from a VJudge Team value."""
    team_text = str(team_raw).strip()
    ascii_match = re.fullmatch(r".*\(([^()]*)\)\s*", team_text)
    if ascii_match:
        return ascii_match.group(1).strip()

    full_width_match = re.fullmatch(r".*（([^（）]*)）\s*", team_text)
    if full_width_match:
        return full_width_match.group(1).strip()

    return team_text


def parse_vjudge_xlsx(path: Path | str) -> list[VJudgeStandingRow]:
    """Parse the first sheet of a VJudge xlsx export."""
    xlsx_path = Path(path)
    try:
        with zipfile.ZipFile(xlsx_path) as archive:
            shared_strings = _load_shared_strings(archive)
            sheet_path = _get_first_sheet_path(archive)
            rows = _load_sheet_rows(archive, sheet_path, shared_strings)
    except VJudgeImportError:
        raise
    except (OSError, KeyError, zipfile.BadZipFile, ElementTree.ParseError) as exc:
        raise VJudgeImportError(
            "Excel 读取失败，请确认文件是有效的 .xlsx 文件。"
        ) from exc

    header = _find_header(rows)
    if header is None:
        raise VJudgeImportError(
            "导入失败：无法识别 Rank 或 Team 列，请检查 VJudge 导出表头。"
        )

    header_row_no, rank_col, team_col = header
    parsed_rows: list[VJudgeStandingRow] = []
    for row_no, values in rows:
        if row_no <= header_row_no:
            continue

        rank_text = values.get(rank_col, "").strip()
        team_text = values.get(team_col, "").strip()
        if not rank_text and not team_text and _row_is_empty(values):
            continue

        rank = _parse_rank(rank_text, row_no)
        if not team_text:
            raise VJudgeImportError(
                "\n".join(
                    [
                        "导入失败：Team 为空",
                        "",
                        f"第 {row_no} 行：",
                        f"Rank = {_display_cell(rank_text)}",
                        "Team = (空)",
                    ]
                )
            )

        candidate = extract_candidate_nickname(team_text)
        if not candidate:
            raise VJudgeImportError(
                "\n".join(
                    [
                        "导入失败：Team 解析后的昵称为空",
                        "",
                        f"第 {row_no} 行：",
                        f"Rank = {rank}",
                        f"Team = {team_text}",
                    ]
                )
            )

        parsed_rows.append(
            VJudgeStandingRow(
                row_no=row_no,
                rank=rank,
                team_raw=team_text,
                candidate_nickname=candidate,
            )
        )

    _validate_duplicate_candidates(parsed_rows)
    if len(parsed_rows) < 2:
        raise VJudgeImportError("导入失败：有效参赛者少于 2 人。")
    return parsed_rows


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        data = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []

    root = ElementTree.fromstring(data)
    values: list[str] = []
    for item in root.findall(f"{{{MAIN_NS}}}si"):
        values.append(
            "".join(
                text_node.text or ""
                for text_node in item.iter()
                if _local_name(text_node.tag) == "t"
            )
        )
    return values


def _get_first_sheet_path(archive: zipfile.ZipFile) -> str:
    workbook_root = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    sheets_node = workbook_root.find(f"{{{MAIN_NS}}}sheets")
    if sheets_node is None:
        raise VJudgeImportError("Excel 读取失败：未找到工作表。")

    first_sheet = None
    for sheet in sheets_node.findall(f"{{{MAIN_NS}}}sheet"):
        first_sheet = sheet
        break
    if first_sheet is None:
        raise VJudgeImportError("Excel 读取失败：未找到工作表。")

    relationship_id = first_sheet.attrib.get(f"{{{REL_NS}}}id")
    if not relationship_id:
        return "xl/worksheets/sheet1.xml"

    rels_root = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for rel in rels_root.findall(f"{{{PACKAGE_REL_NS}}}Relationship"):
        if rel.attrib.get("Id") == relationship_id:
            target = rel.attrib.get("Target", "")
            if target.startswith("/"):
                return target.lstrip("/")
            if target.startswith("xl/"):
                return posixpath.normpath(target)
            return posixpath.normpath(posixpath.join("xl", target))

    return "xl/worksheets/sheet1.xml"


def _load_sheet_rows(
    archive: zipfile.ZipFile, sheet_path: str, shared_strings: list[str]
) -> list[tuple[int, dict[int, str]]]:
    sheet_root = ElementTree.fromstring(archive.read(sheet_path))
    sheet_data = sheet_root.find(f"{{{MAIN_NS}}}sheetData")
    if sheet_data is None:
        return []

    rows: list[tuple[int, dict[int, str]]] = []
    for row_index, row_node in enumerate(
        sheet_data.findall(f"{{{MAIN_NS}}}row"), start=1
    ):
        row_no = _parse_row_number(row_node.attrib.get("r"), default=row_index)
        values: dict[int, str] = {}
        fallback_col = 1
        for cell_node in row_node.findall(f"{{{MAIN_NS}}}c"):
            cell_ref = cell_node.attrib.get("r", "")
            col_no = _column_number_from_cell_ref(cell_ref) or fallback_col
            values[col_no] = _read_cell_text(cell_node, shared_strings).strip()
            fallback_col = col_no + 1
        rows.append((row_no, values))
    return rows


def _read_cell_text(cell_node: ElementTree.Element, shared_strings: list[str]) -> str:
    cell_type = cell_node.attrib.get("t")
    if cell_type == "inlineStr":
        inline_node = cell_node.find(f"{{{MAIN_NS}}}is")
        if inline_node is None:
            return ""
        return "".join(
            text_node.text or ""
            for text_node in inline_node.iter()
            if _local_name(text_node.tag) == "t"
        )

    value_node = cell_node.find(f"{{{MAIN_NS}}}v")
    raw_value = value_node.text if value_node is not None else ""
    if raw_value is None:
        return ""

    if cell_type == "s":
        try:
            return shared_strings[int(raw_value)]
        except (ValueError, IndexError):
            return raw_value
    return str(raw_value)


def _find_header(rows: list[tuple[int, dict[int, str]]]) -> tuple[int, int, int] | None:
    scanned = 0
    for row_no, values in rows:
        if _row_is_empty(values):
            continue
        scanned += 1
        rank_col = None
        team_col = None
        for col_no, value in values.items():
            normalized = _normalize_header(value)
            if normalized in RANK_HEADERS and rank_col is None:
                rank_col = col_no
            if normalized in TEAM_HEADERS and team_col is None:
                team_col = col_no
        if rank_col is not None and team_col is not None:
            return row_no, rank_col, team_col
        if scanned >= MAX_HEADER_SCAN_ROWS:
            break
    return None


def _parse_rank(rank_text: str, row_no: int) -> int:
    normalized = str(rank_text).strip()
    rank: int | None = None
    if re.fullmatch(r"\d+", normalized):
        rank = int(normalized)
    else:
        try:
            decimal_value = Decimal(normalized)
        except InvalidOperation:
            decimal_value = None
        if (
            decimal_value is not None
            and decimal_value.is_finite()
            and decimal_value == decimal_value.to_integral()
        ):
            rank = int(decimal_value)

    if rank is None or rank <= 0:
        raise VJudgeImportError(
            "\n".join(
                [
                    "导入失败：Rank 非法",
                    "",
                    f"第 {row_no} 行：",
                    f"Rank = {_display_cell(normalized)}",
                    "原因：Rank 必须是正整数。",
                ]
            )
        )
    return rank


def _validate_duplicate_candidates(rows: list[VJudgeStandingRow]) -> None:
    first_seen: dict[str, VJudgeStandingRow] = {}
    for row in rows:
        previous = first_seen.get(row.candidate_nickname)
        if previous is not None:
            raise VJudgeImportError(
                "\n".join(
                    [
                        "导入失败：昵称重复",
                        "",
                        f"第 {previous.row_no} 行和第 {row.row_no} 行都解析为："
                        f"{row.candidate_nickname}",
                        "",
                        "请检查榜单或昵称映射规则。",
                    ]
                )
            )
        first_seen[row.candidate_nickname] = row


def _parse_row_number(row_ref: str | None, default: int) -> int:
    if row_ref and row_ref.isdigit():
        return int(row_ref)
    return default


def _column_number_from_cell_ref(cell_ref: str) -> int | None:
    match = re.match(r"([A-Za-z]+)", cell_ref)
    if not match:
        return None

    col_no = 0
    for char in match.group(1).upper():
        col_no = col_no * 26 + ord(char) - ord("A") + 1
    return col_no


def _normalize_header(value: str) -> str:
    return str(value).strip().casefold()


def _row_is_empty(values: dict[int, str]) -> bool:
    return all(not str(value).strip() for value in values.values())


def _display_cell(value: str) -> str:
    value = str(value).strip()
    return value if value else "(空)"


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag

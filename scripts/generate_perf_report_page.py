#!/usr/bin/env python3
import argparse
from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any, Dict, List, Optional, Tuple, cast
from jinja2 import Template

# skip 'input' columns. They are included in the header and just blow the table
EXCLUDE_COLUMNS = frozenset({
    'scale',
    'duration',
    'number_of_clients',
    'number_of_threads',
    'init_start_timestamp',
    'init_end_timestamp',
    'run_start_timestamp',
    'run_end_timestamp',
})

KEY_EXCLUDE_FIELDS = frozenset({
    'init_start_timestamp',
    'init_end_timestamp',
    'run_start_timestamp',
    'run_end_timestamp',
})
NEGATIVE_COLOR = 'negative'
POSITIVE_COLOR = 'positive'


@dataclass
class SuitRun:
    revision: str
    values: Dict[str, Any]


@dataclass
class SuitRuns:
    platform: str
    suit: str
    common_columns: List[Tuple[str, str]]
    value_columns: List[str]
    runs: List[SuitRun]


@dataclass
class RowValue:
    value: str
    color: str
    ratio: str


def get_columns(values: List[Dict[Any, Any]]) -> Tuple[List[Tuple[str, str]], List[str]]:
    value_columns = []
    common_columns = []
    for item in values:
        if item['name'] in KEY_EXCLUDE_FIELDS:
            continue
        if item['report'] != 'test_param':
            value_columns.append(cast(str, item['name']))
        else:
            common_columns.append((cast(str, item['name']), cast(str, item['value'])))
    value_columns.sort()
    common_columns.sort(key=lambda x: x[0])  # sort by name
    return common_columns, value_columns


def format_ratio(ratio: float, report: str) -> Tuple[str, str]:
    color = ''
    sign = '+' if ratio > 0 else ''
    if abs(ratio) < 0.05:
        return f'&nbsp({sign}{ratio:.2f})', color

    if report not in {'test_param', 'higher_is_better', 'lower_is_better'}:
        raise ValueError(f'Unknown report type: {report}')

    if report == 'test_param':
        return f'{ratio:.2f}', color

    if ratio > 0:
        if report == 'higher_is_better':
            color = POSITIVE_COLOR
        elif report == 'lower_is_better':
            color = NEGATIVE_COLOR
    elif ratio < 0:
        if report == 'higher_is_better':
            color = NEGATIVE_COLOR
        elif report == 'lower_is_better':
            color = POSITIVE_COLOR

    return f'&nbsp({sign}{ratio:.2f})', color


def extract_value(name: str, suit_run: SuitRun) -> Optional[Dict[str, Any]]:
    for item in suit_run.values['data']:
        if item['name'] == name:
            return cast(Dict[str, Any], item)
    return None


def get_row_values(columns: List[str], run_result: SuitRun,
                   prev_result: Optional[SuitRun]) -> List[RowValue]:
    row_values = []
    for column in columns:
        current_value = extract_value(column, run_result)
        if current_value is None:
            # should never happen
            raise ValueError(f'{column} not found in {run_result.values}')

        value = current_value["value"]
        if isinstance(value, float):
            value = f'{value:.2f}'

        if prev_result is None:
            row_values.append(RowValue(value, '', ''))
            continue

        prev_value = extract_value(column, prev_result)
        if prev_value is None:
            # this might happen when new metric is added and there is no value for it in previous run
            # let this be here, TODO add proper handling when this actually happens
            raise ValueError(f'{column} not found in previous result')
        ratio = float(value) / float(prev_value['value']) - 1
        ratio_display, color = format_ratio(ratio, current_value['report'])
        row_values.append(RowValue(value, color, ratio_display))
    return row_values


@dataclass
class SuiteRunTableRow:
    revision: str
    values: List[RowValue]


def prepare_rows_from_runs(value_columns: List[str], runs: List[SuitRun]) -> List[SuiteRunTableRow]:
    rows = []
    prev_run = None
    for run in runs:
        rows.append(
            SuiteRunTableRow(revision=run.revision,
                             values=get_row_values(value_columns, run, prev_run)))
        prev_run = run

    return rows


def main(args: argparse.Namespace) -> None:
    input_dir = Path(args.input_dir)
    grouped_runs: Dict[str, SuitRuns] = {}
    # we have files in form: <ctr>_<rev>.json
    # fill them in the hashmap so we have grouped items for the
    # same run configuration (scale, duration etc.) ordered by counter.
    for item in sorted(input_dir.iterdir(), key=lambda x: int(x.name.split('_')[0])):
        run_data = json.loads(item.read_text())
        revision = run_data['revision']

        for suit_result in run_data['result']:
            key = "{}{}".format(run_data['platform'], suit_result['suit'])
            # pack total duration as a synthetic value
            total_duration = suit_result['total_duration']
            suit_result['data'].append({
                'name': 'total_duration',
                'value': total_duration,
                'unit': 's',
                'report': 'lower_is_better',
            })
            common_columns, value_columns = get_columns(suit_result['data'])

            grouped_runs.setdefault(
                key,
                SuitRuns(
                    platform=run_data['platform'],
                    suit=suit_result['suit'],
                    common_columns=common_columns,
                    value_columns=value_columns,
                    runs=[],
                ),
            )

            grouped_runs[key].runs.append(SuitRun(revision=revision, values=suit_result))
    context = {}
    for result in grouped_runs.values():
        suit = result.suit
        context[suit] = {
            'common_columns': result.common_columns,
            'value_columns': result.value_columns,
            'platform': result.platform,
            # reverse the order so newest results are on top of the table
            'rows': reversed(prepare_rows_from_runs(result.value_columns, result.runs)),
        }

    template = Template((Path(__file__).parent / 'perf_report_template.html').read_text())

    Path(args.out).write_text(template.render(context=context))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-dir',
        dest='input_dir',
        required=True,
        help='Directory with jsons generated by the test suite',
    )
    parser.add_argument('--out', required=True, help='Output html file path')
    args = parser.parse_args()
    main(args)

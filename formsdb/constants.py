"""
This module contains constants for the AMPSCZ project.
"""

from typing import List

networks: List[str] = ["Pronet", "Prescient"]
# networks: List[str] = ["Prescient"]
# networks: List[str] = ["Pronet"]

visit_order: List[str] = [
    "screening",
    "baseline",
    "month_1",
    "month_2",
    "month_3",
    "month_4",
    "month_5",
    "month_6",
    "month_7",
    "month_8",
    "month_9",
    "month_10",
    "month_11",
    "month_12",
    "month_18",
    "month_24",
]

upenn_visit_order: List[str] = [
    "baseline",
    "month_2",
    "month_6",
    "month_12",
    "month_18",
    "month_24",
]

dp_dash_required_cols = [
    "reftime",
    "day",
    "timeofday",
    "weekday",
]

upenn_tests: List[str] = [
    "mpract",  # Motor Praxis Test
    "spcptn90",  # Short Penn Continuous Performance Test
    "er40_d",  # Penn Emotion Recognition Task
    "sfnb2",  # Short Fractal N-back
    "digsym",  # Digit Symbol Test
    "svolt",  # Short Visual Object Learning and Memory
    "sctap",  # Short Computerized Finger Tapping Test
    "spllt",  # Short Penn List Learning Test
]

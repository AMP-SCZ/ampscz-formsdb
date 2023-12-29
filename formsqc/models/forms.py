from datetime import datetime


class Forms:
    """
    Represents a REDCap / RPMS form.

    Attributes:
        subject_id (str): The ID of the subject.
        form_name (str): The name of the form.
        event_name (str): The name of the event / timepoint: e.g. "baseline_arm_1".
        form_data (dict): The data submitted in the form.
        source_m_date (datetime): The date the form data was retrieved from the source.
        metadata (dict): Additional metadata associated with the form.
    """

    def __init__(
        self,
        subject_id: str,
        form_name: str,
        event_name: str,
        form_data: dict,
        source_m_date: datetime,
        metadata: dict,
    ):
        self.subject_id = subject_id
        self.form_name = form_name
        self.event_name = event_name
        self.form_data = form_data
        self.source_m_date = source_m_date
        self.metadata = metadata

    def __str__(self):
        return f"{self.subject_id} {self.form_name} {self.event_name}"

    def __repr__(self):
        return f"{self.subject_id} {self.form_name} {self.event_name}"

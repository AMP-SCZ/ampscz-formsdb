from datetime import datetime


class UpennForms:
    """
    Represents a form from the UPenn REDCap

    Attributes:
        subject_id (str): The ID of the subject.
        event_name (str): The name of the event.
        event_type (str): The type of the event.
        form_data (dict): The data associated with the form.
        source_m_date (datetime): The source modification date of the form.
    """

    def __init__(
        self,
        subject_id: str,
        event_name: str,
        event_type: str,
        form_data: dict,
        source_m_date: datetime,
    ):
        self.subject_id = subject_id
        self.event_name = event_name
        self.event_type = event_type
        self.form_data = form_data
        self.source_m_date = source_m_date

    def __str__(self):
        return f"UpennForms: {self.subject_id} {self.event_name} {self.event_type}"

    def __repr__(self):
        return self.__str__()

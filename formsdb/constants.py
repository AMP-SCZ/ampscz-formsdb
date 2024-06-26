"""
This module contains constants for the AMPSCZ project.
"""

from typing import List, Dict

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

# RPMS ClientStatus CSVs use the following visit names,
# which are mapped to the respective REDCap event names.
client_status_visit_mapping: Dict[str, str] = {
    "Baseline": "baseline",
    "Month 1": "month_1",
    "Month 2": "month_2",
    "Month 3": "month_3",
    "Month 4": "month_4",
    "Month 5": "month_5",
    "Month 6": "month_6",
    "Month 7": "month_7",
    "Month 8": "month_8",
    "Month 9": "month_9",
    "Month 10": "month_10",
    "Month 11": "month_11",
    "Month 12": "month_12",
    "Month 18": "month_18",
    "Month 24": "month_24",
}

upenn_visit_order: List[str] = [
    "baseline",
    "month_2",
    "month_6",
    "month_12",
    "month_24",
]

dp_dash_required_cols: List[str] = [
    "reftime",
    "day",
    "timeofday",
    "weekday",
    "subject_id",
]

skip_adding_nr: List[str] = [
    "recruitment_status",
    "recruitment_status_v2",
    "converted",
    "withdrawal_status",
    "timepoint",
    "visit_status",
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


form_name_to_abbrv: Dict[str, str] = {
    "informed_consent_run_sheet": "chric",
    "informed_reconsent": "chric",
    "missing_data": "chrmiss",
    "recruitment_source": "chrrecruit",
    "coenrollment_form": "chrcoen",
    "lifetime_ap_exposure_screen": "chrap",
    "past_pharmaceutical_treatment": "chrpharm",
    "current_pharmaceutical_treatment_floating_med_125": "chrpharm",
    "current_pharmaceutical_treatment_floating_med_2650": "chrpharm",
    "resource_use_log": "chrrul",
    "sociodemographics": "chrdemo",
    "psychosocial_treatment_form": "chrpsychsoc",
    "health_conditions_medicalpsychiatric_history": "chrmed",
    "health_conditions_genetics_fluid_biomarkers": "chrhealth",
    "scid5_psychosis_mood_substance_abuse": "chrscid",
    "scid5_schizotypal_personality_sciddpq": "chrschizotypal",
    "traumatic_brain_injury_screen": "chrtbi",
    "family_interview_for_genetic_studies_figs": "chrfigs",
    "adverse_events": "chrae",
    "sofas_screening": "chrsofas",
    "sofas_followup": "chrsofas",
    "psychs_av_recording_run_sheet": "psychs_av_recording_run_sheet",
    "psychs_p1p8": "chrpsychs_scr",
    "psychs_p9ac32": "chrpsychs_scr",
    "psychs_p1p8_fu": "chrpsychs_fu",
    "psychs_p9ac32_fu": "chrpsychs_fu",
    "psychs_p1p8_fu_hc": "chrpsychs_fu",
    "psychs_p9ac32_fu_hc": "chrpsychs_fu",
    "inclusionexclusion_criteria_review": "chrcrit",
    "premorbid_adjustment_scale": "chrpas",
    "perceived_discrimination_scale": "chrdim",
    "pubertal_developmental_scale": "chrpds",
    "penncnb": "chrpenn",
    "iq_assessment_wasiii_wiscv_waisiv": "chriq",
    "premorbid_iq_reading_accuracy": "chrpreid",
    "eeg_run_sheet": "chreeg",
    "mri_run_sheet": "chrmri",
    "mri_incidental_findings_run_sheet": "chrif",
    "digital_biomarkers_mindlamp_onboarding": "chrdbb",
    "digital_biomarkers_mindlamp_checkin": "chrdig",
    "digital_biomarkers_mindlamp_end_of_12month__study_p": "chrdbe",
    "digital_biomarkers_axivity_onboarding": "chrax",
    "digital_biomarkers_axivity_checkin": "chraxci",
    "digital_biomarkers_axivity_end_of_12month__study_pe": "chraxe",
    "current_health_status": "chrchs",
    "daily_activity_and_saliva_sample_collection": "chrsaliva",
    "blood_sample_preanalytic_quality_assurance": "chrblood",
    "cbc_with_differential": "chrcbc",
    "gcp_cbc_with_differential": "chrgcp",
    "gcp_current_health_status": "chrgcp",
    "speech_sampling_run_sheet": "chrspeech",
    "nsipr": "chrnsipr",
    "cdss": "chrcdss",
    "oasis": "chroasis",
    "assist": "chrassist",
    "cssrs_baseline": "chrcssrsb",
    "cssrs_followup": "chrcssrsfu",
    "perceived_stress_scale": "chrpss",
    "global_functioning_social_scale": "chrgfss",
    "global_functioning_social_scale_followup": "chrgfssfu",
    "global_functioning_role_scale": "chrgfrs",
    "global_functioning_role_scale_followup": "chrgfrsfu",
    "item_promis_for_sleep": "chrpromis",
    "bprs": "chrbprs",
    "pgis": "chrpgi",
    "psychosis_polyrisk_score": "chrpps",
    "ra_prediction": "chrpred",
    "guid_form": "chrguid",
    "conversion_form": "chrconv",
}

# Similar List at:
# https://github.com/AMP-SCZ/utility/blob/main/rpms_form_labels.csv
rmps_to_redcap_form_name: Dict[str, str] = {
    "Actigraphy": "digital_biomarkers_axivity_checkin",
    "AdverseEvents": "adverse_events",
    "ASSIST": "assist",
    "BloodSpecimenPQA": "blood_sample_preanalytic_quality_assurance",
    "BPRS": "bprs",
    "CBC": "cbc_with_differential",
    "CBC_GCP": "gcp_cbc_with_differential",
    "CDSS": "cdss",
    "Coenrollment": "coenrollment_form",
    "ConversionForm": "conversion_form",
    "CSSRS": "cssrs_followup",
    "CurrentHealthStatus": "current_health_status",
    "CurrentHealthStatus_GCP": "gcp_current_health_status",
    "Demographics": "sociodemographics",
    "EEG": "eeg_run_sheet",
    "EMA": "digital_biomarkers_mindlamp_checkin",
    "FIGS": "family_interview_for_genetic_studies_figs",
    "GlobalFuncRS": "global_functioning_role_scale",
    "GlobalFuncRSFUP": "global_functioning_role_scale_followup",
    "GlobalFuncSS": "global_functioning_social_scale",
    "GlobalFuncSSFUP": "global_functioning_social_scale_followup",
    "GUID": "guid_form",
    "HealthConditions": "health_conditions_genetics_fluid_biomarkers",
    "InclusionExclusionCriteriaReview": "inclusionexclusion_criteria_review",
    "IQ Assessment": "iq_assessment_wasiii_wiscv_waisiv",
    "LifetimeAP": "lifetime_ap_exposure_screen",
    "MissingData": "missing_data",
    "MRI": "mri_run_sheet",
    "MRI-Incidental": "mri_incidental_findings_run_sheet",
    "NSI-PR": "nsipr",
    "OASIS": "oasis",
    "PAS": "premorbid_adjustment_scale",
    "PDQ": "perceived_discrimination_scale",
    "PDS": "pubertal_developmental_scale",
    "PennCNB": "penncnb",
    "PGI-S": "pgis",
    "PharmaceuticalTreatment": "past_pharmaceutical_treatment",
    "PICF_HealthyControl_Self_V2": "informed_consent_run_sheet",
    "PICF_HealthyControl_Self_V3": "informed_consent_run_sheet",
    "PICF_UHR_Self_V2": "informed_consent_run_sheet",
    "PICF_UHR_Self_V3": "informed_consent_run_sheet",
    "PICF_UHR_ParentGuardian_V2": "informed_consent_run_sheet",
    "PICF_UHR_ParentGuardian_V3": "informed_consent_run_sheet",
    "PICF_HealthyControl_ParentGuardian_V2": "informed_consent_run_sheet",
    "PICF_HealthyControl_ParentGuardian_V3": "informed_consent_run_sheet",
    "PPS": "psychosis_polyrisk_score",
    "PremorbidIQ": "premorbid_iq_reading_accuracy",
    "PROMIS-SD": "item_promis_for_sleep",
    "PSS": "perceived_stress_scale",
    "PsychosocialTreatment": "psychosocial_treatment_form",
    "PSYCHSP1P8": "psychs_p1p8_fu",  # Can also be "psychs_p1p8" if it's the first form, or "psychs_p1p8_fu_hc"
    "PSYCHSP9": "psychs_p9ac32_fu",  # Can also be "psychs_p9ac32" if it's the first form or "psychs_p9ac32_fu_hc"
    # "PSYCHSP1P8_v1": "psychs_p1p8_fu",
    # "PSYCHSP9_v1": "psychs_p9ac32_fu",
    "PsychsRunsheet": "psychs_av_recording_run_sheet",
    "RA Prediction": "ra_prediction",
    "RecruitmentSource": "recruitment_source",
    "RUL": "resource_use_log",
    "Saliva": "daily_activity_and_saliva_sample_collection",
    "SCID": "scid5_psychosis_mood_substance_abuse",
    # "SCIDv1": "scid5_psychosis_mood_substance_abuse",
    "SCID5PD": "scid5_schizotypal_personality_sciddpq",
    "SOFAS": "sofas_followup",  # Can also be "sofas_screening" if it's the first form
    "SpeechSampling": "speech_sampling_run_sheet",
    "TBI": "traumatic_brain_injury_screen",
}

rpms_to_redcap_event: Dict[int, str] = {
    1: "screening",
    2: "baseline",
    3: "month_1",
    4: "month_2",
    5: "month_3",
    6: "month_4",
    7: "month_5",
    8: "month_6",
    9: "month_7",
    10: "month_8",
    11: "month_9",
    12: "month_10",
    13: "month_11",
    14: "month_12",
    15: "month_18",
    16: "month_24",
    98: "conversion",
    99: "floating",
    100: "screening",  # Self Consent
    101: "screening",  # Parental Consent
}

rpms_entry_status_map: Dict[int, str] = {
    0: "Incomplete",
    1: "Partial",
    2: "Complete",
    3: "N/A",
    4: "Missing",
}

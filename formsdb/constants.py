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
    "scid5_psychosis_mood_substance_abuse": "chrscid",
    "traumatic_brain_injury_screen": "chrtbi",
    "family_interview_for_genetic_studies_figs": "chrfigs",
    "adverse_events": "chrae",
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
    "digital_biomarkers_mindlamp_end_of_12month_study_p": "chrdbe",
    "digital_biomarkers_axivity_onboarding": "chrax",
    "digital_biomarkers_axivity_checkin": "chraxci",
    "digital_biomarkers_axivity_end_of_12month_study_pe": "chraxe",
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

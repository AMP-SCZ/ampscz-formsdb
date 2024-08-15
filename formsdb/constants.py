"""
This module contains constants for the AMPSCZ project.
"""

from typing import Dict, List, Union

networks: List[str] = ["Pronet", "Prescient"]
networks_legacy: List[str] = ["ProNET", "PRESCIENT"]
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
    "premorbid_iq_reading_accuracy": "chrpreiq",
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
    17: "other_study",
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

# missingness detection
required_form_variables: Dict[str, List[str]] = {
    "perceived_discrimination_scale": [
        "chrdim_dim_yesno_q1_1",
        "chrdlm_dim_yesno_q1_2",
        "chrdlm_dim_sex",
        "chrdlm_dim_yesno_age",
        "chrdlm_dim_yesno_q4_1",
        "chrdlm_dim_yesno_q5",
        "chrdlm_dim_yesno_q3",
        "chrdlm_dim_yesno_q6",
        "chrdlm_dim_yesno_other",
    ],
    "nsipr": [
        "chrnsipr_item1_rating",
        "chrnsipr_item2_rating",
        "chrnsipr_item3_rating",
        "chrnsipr_item4_rating",
        "chrnsipr_item5_rating",
        "chrnsipr_item6_rating",
        "chrnsipr_item7_rating",
        "chrnsipr_item8_rating",
        "chrnsipr_item9_rating",
        "chrnsipr_item10_rating",
        "chrnsipr_item11_rating",
    ],
    "cdss": [
        "chrcdss_calg1",
        "chrcdss_calg2",
        "chrcdss_calg3",
        "chrcdss_calg4",
        "chrcdss_calg5",
        "chrcdss_calg6",
        "chrcdss_calg7",
        "chrcdss_calg8",
        "chrcdss_calg9",
    ],
    "osasis": [
        "chroasis_oasis_1",
        "chroasis_oasis_2",
        "chroasis_oasis_3",
        "chroasis_oasis_4",
        "chroasis_oasis_5",
    ],
    "pss": [
        "chrpss_pssp1_1",
        "chrpss_pssp1_2",
        "chrpss_pssp1_3",
        "chrpss_pssp2_1",
        "chrpss_pssp2_2",
        "chrpss_pssp2_3",
        "chrpss_pssp2_4",
        "chrpss_pssp2_5",
        "chrpss_pssp3_1",
        "chrpss_pssp3_4",
    ],
    "item_promis_for_sleep": [
        "chrpromis_sleep109",
        "chrpromis_sleep116",
        "chrpromis_sleep20",
        "chrpromis_sleep44",
        "chrpromise_sleep108",
        "chrpromis_sleep72",
        "chrpromis_sleep67",
        "chrpromis_sleep115",
    ],
    "bprs": ["chrbprs_bprs_total"],
    "cssrs_baseline": [
        "chrcssrsb_si1l",
        "chrcssrsb_si2l",
        "chrcssrsb_si3l",
        "chrcssrsb_si4l",
        "chrcssrsb_si5l",
        "chrcssrsb_css_sim1",
        "chrcssrsb_css_sim2",
        "chrcssrsb_css_sim3",
        "chrcssrsb_css_sim4",
        "chrcssrsb_css_sim5",
    ],
    "cssrs_followup": [
        "chrcssrsfu_si1l",
        "chrcssrsfu_si2l",
        "chrcssrsfu_si3l",
        "chrcssrsfu_si4l",
        "chrcssrsfu_si5l",
        "chrcssrsfu_css_sim1",
        "chrcssrsfu_css_sim2",
        "chrcssrsfu_css_sim3",
        "chrcssrsfu_css_sim4",
        "chrcssrsfu_css_sim5",
    ],
    "sofas": [
        "chrsofas_premorbid",
        "chrsofas_currscore12mo",
        "chrsofas_currscore",
        "chrsofas_lowscore",
    ],
    "informed_consent_run_sheet": [
        "chric_consent_date",
        "chric_passive",
        "chric_surveys",
        "chric_actigraphy",
    ],
    "global_functioning_role_scale": [
        "chrgfr_prompt2",
        "chrgfr_prompt3",
        "chrgfr_prompt4",
        "chrgfr_gf_primaryrole",
        "chrgfr_gf_role_scole",
        "chrgfr_gf_role_low",
        "chrgfr_gf_role_high",
    ],
    "global_functioning_social_scale": [
        "chrgfs_overallsat",
        "chrgfs_gf_social_scale",
        "chrgfs_gf_social_low",
        "chrgfs_gf_social_high",
    ],
    "penncnb": [
        "chrpenn_complete",
        "chrpenn_missing_1",
    ],
    "premorbid_iq_reading_accuracy": [
        "chrpreiq_reading_task",
        "chrpreiq_total_raw",
        "chrpreiq_standard_score",
    ],
    "iq_assessment_wasiii_wiscv_waisiv": [
        "chriq_assessment",
        "chriq_vocab_raw",
        "chriq_matrix_raw",
        "chriq_fsiq",
    ],
}

# Conditional missing:
# pubertal_developmental_scale : Gender
# premorbid_adjustment_scale : Age
conditionally_required_form_variables: Dict[
    str, Dict[str, Union[List[str], Dict[str, List[str]]]]
] = {
    "pubertal_developmental_scale": {
        "required": [
            "chrpds_pds_1_p",
            "chrpds_pds_1_p",
            "chrpds_pds_2_p",
            "chrpds_pds_3_p",
        ],
        "gender": {
            "male": ["chrpds_pds_m4_p", "chrpds_pds_m5_p"],
            "female": ["chrpds_pds_f4_p", "chrpds_pds_f5b_p"],
        },
    },
    "premorbid_adjustment_scale": {
        "age": {
            "< 15": [
                "chrpas_pmod_child1",
                "chrpas_pmod_child2",
                "chrpas_pmod_child3",
                "chrpas_pmod_child4",
            ],
            "< 18": [
                "chrpas_pmod_adol_early1",
                "chrpas_pmod_adol_early2",
                "chrpas_pmod_adol_early3",
                "chrpas_pmod_adol_early4",
                "chrpas_pmod_adol_early5",
            ],
            "< 19": [
                "chrpas_pmod_adol_late1",
                "chrpas_pmod_adol_late2",
                "chrpas_pmod_adol_late3",
                "chrpas_pmod_adol_late4",
                "chrpas_pmod_adol_late5",
            ],
            "> 18": [
                "chrpas_pmod_adult1",
                "chrpas_pmod_adult2",
                "chrpas_pmod_adult3v1",  # OR
                "chrpas_pmod_adult3v3",
            ],
        }
    },
}

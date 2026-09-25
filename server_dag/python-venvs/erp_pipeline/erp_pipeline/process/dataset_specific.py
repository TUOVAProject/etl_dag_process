from __future__ import annotations
import pandas as pd

def resolve_part_extrafields_by_group_name(
    extraFieldGroups: pd.DataFrame,
    extraFieldTemplates: pd.DataFrame,
    extraFieldOptionTemplates: pd.DataFrame,
    extraFields: pd.DataFrame,
    group_name: str
) -> pd.DataFrame:

    groups = extraFieldGroups[
        extraFieldGroups["Name"].str.lower() == group_name.lower()
    ]
    if groups.empty:
        raise ValueError(f"ExtraFieldGroup '{group_name}' ei löytynyt")

    group_ids = groups["Id"]

    templates = extraFieldTemplates[
        extraFieldTemplates["ParentId"].isin(group_ids)
    ]
    if templates.empty:
        raise ValueError(f"ExtraFieldTemplates puuttuu ryhmälle '{group_name}'")


    options = extraFieldOptionTemplates[
        extraFieldOptionTemplates["ExtraFieldTemplateId"].isin(templates["Id"])
    ]
    if options.empty:
        raise ValueError(f"ExtraFieldOptionTemplates puuttuu ryhmälle '{group_name}'")

    fields = extraFields[
        extraFields["SelectedOptionId"].isin(options["Id"])
    ].copy()

    if fields.empty:
        return pd.DataFrame(
            columns=[
                "PartId",
                "ExtraFieldTemplateId",
                "ExtraFieldOptionTemplateId"
            ]
        )

    parent_classes = fields["ParentClass"].unique()
    if len(parent_classes) != 1:
        raise ValueError(
            f"Ryhmä '{group_name}' kohdistuu useaan ParentClassiin: {parent_classes}"
        )
    
    fields = fields.rename(columns={
        "ParentId": "PartId",
        "SelectedOptionId": "ExtraFieldOptionTemplateId"
    })

    fields = fields.merge(
        options[["Id", "ExtraFieldTemplateId", "Description","Code"]],
        left_on="ExtraFieldOptionTemplateId",
        right_on="Id",
        how="left",
        suffixes=("", "_Option")
    )

    return fields[[
        "PartId",
        "ExtraFieldTemplateId",
        "ExtraFieldOptionTemplateId",
        "Description",
        "Code"
    ]]


def fill_value_from_parent_row(
    df: pd.DataFrame,
    id_col: str = "OrderRowId",
    parent_col: str = "ParentRowId",
    value_col: str = "PartId",
    max_iterations: int = 5
) -> pd.DataFrame:
    """
    Täyttää puuttuvat PartId-arvot hakemalla ne emo-riviltä ParentOrderRowId:n
    kautta (esim. kokoonpanon alarivit, jotka eivät kanna omaa PartId:tä).

    Iteroi useamman kierroksen, jos ketju on moniportainen (rivi -> emorivi ->
    isoisorivi jne.), ja pysähtyy kun mitään uutta ei enää saada täytettyä.
    """
    if id_col not in df.columns or parent_col not in df.columns or value_col not in df.columns:
        print(
            "fill_partid_from_parent_row: sarake puuttuu (%s/%s/%s), fallback ohitetaan",
            id_col, parent_col, value_col
        )
        return df

    df = df.copy()

    for _ in range(max_iterations):
        missing_mask = df[value_col].isna() & df[parent_col].notna()
        if not missing_mask.any():
            break

        lookup = df.set_index(id_col)[value_col]
        filled = df.loc[missing_mask, parent_col].map(lookup)

        if filled.notna().sum() == 0:
            break

        df.loc[missing_mask, value_col] = filled

    return df
import pandas as pd

from utils.titlecase import smart_title_case
from transforms.resolve_categorization import TITLECASE_RULEBOOK_COLS


def test_smart_title_case_acronyms_and_small_words():
    assert smart_title_case("Currency and Coin Deposited") == "Currency and Coin Deposited"
    assert smart_title_case("City of Sacramento") == "City of Sacramento"
    assert smart_title_case("COMCAST") == "COMCAST"  # preserved by acronyms list
    assert smart_title_case("ewb 8452") == "EWB 8452"
    assert smart_title_case("nbcu swd") == "NBCU SWD"
    assert smart_title_case("cc 71000") == "CC 71000"
    assert smart_title_case("delivery cogs:vehicle compliance") == "Delivery COGS:Vehicle Compliance"
    assert smart_title_case("spencer young law pc") == "Spencer Young Law PC"
    assert smart_title_case("oil changer hq") == "Oil Changer HQ"
    assert smart_title_case("e-z*pass toll") == "E-Z*PASS Toll"
    assert smart_title_case("gain fcu daily paym") == "Gain FCU Daily Paym"
    assert smart_title_case("unknown") == "unknown"  # untouched sentinel casing
    assert smart_title_case("") == ""
    assert smart_title_case(None) is None


def test_titlecase_rulebook_columns_excludes_entity_and_sub_account():
    df = pd.DataFrame(
        {
            "payee_vendor": ["nbcu swd"],
            "entity_qbo": ["tpH786 entity"],
            "qbo_sub_account": ["custom sub"],
        }
    )

    for col in TITLECASE_RULEBOOK_COLS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: smart_title_case(v) if isinstance(v, str) else v)

    assert df.loc[0, "payee_vendor"] == "NBCU SWD"
    assert df.loc[0, "entity_qbo"] == "tpH786 entity"  # excluded from title-case
    assert df.loc[0, "qbo_sub_account"] == "custom sub"  # excluded from title-case

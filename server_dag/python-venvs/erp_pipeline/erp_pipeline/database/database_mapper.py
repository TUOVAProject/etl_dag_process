from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from erp_pipeline.database.connection import get_engine
from erp_pipeline.database.writers import save_df
from erp_pipeline.helpers.type_helpers import ticks_to_minutes,to_hhmmss,to_int64,get_date_key,shape_and_types
from erp_pipeline.model.load_mode import LoadMode
from erp_pipeline.load_data.data_loaders import load_latest

STAGED_PATH = Path("/opt/airflow/data/results/staged")
COMBINED_PATH = Path("/opt/airflow/data/results/combined")

DIM_CONFLICT_COLUMNS = {
    "dim_workcenter": ["workcenter_id"],
    "dim_article": ["article_id"],
    "dim_customer": ["customer_id"],
    "dim_order_type": ["order_type_id"],
}

def save_or_raise(df: pd.DataFrame, table_name, conflict_columns) -> None:
    engine = get_engine()
    try:
        with engine.begin() as conn:
            save_df(df, table_name, conn, conflict_columns=conflict_columns)
            print(f"{table_name} saved!")
    except Exception as e:
        print(f"save_df epäonnistui ({table_name}): %s", str(e))
        raise RuntimeError(f"save_df epäonnistui ({table_name}) — katso app.log") from None

def process_and_stage_fact_machine_events(combined_work_interval_path: Path):
    print("process_and_stage_fact_machine_events")
    combined_work_intervals = pd.read_parquet(combined_work_interval_path)
    duration = ticks_to_minutes(combined_work_intervals["WorkedTime"])

    
    handle_unit_time = np.where(
    combined_work_intervals["ReportedQuantity"] <= 0,np.nan,
    duration / combined_work_intervals["ReportedQuantity"]
    )

    fact_machine_events = pd.DataFrame({
        # Konetapahtuman yksilöivä uniikki avain.
        "erp_id": to_int64(combined_work_intervals["Id"]),
        #Tällä erotetaan erilaiset konetapahtumat toisistaan. Yhdistyy dim_event_type dimensiotauluun, jossa tapahtumat kuvattu. 
        "event_type_id": 1,
        #Merkataan datan lähde. Yhdistyy dim_source tauluun. Oletuksena 1 = STAMPING, 2 = MACHINE API, 3 = SENSOR
        "source_id" : 1,
        #Yhdistää konetapahtuman jonkin työtilauksen vaiheeseen eli fact_workphases taulun riviin.
        "workphase_id": to_int64(combined_work_intervals["OperationId"]),
        #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "work_number" : combined_work_intervals["OrderNumber"],
        #Työtilauksen tunniste. Ei ole erikseen dimensiotaulua.
        "work_id" : to_int64(combined_work_intervals["ManufacturingOrderId"]),
        #Koneen/työkeskuksen tunniste. Yhdistyy dim_machine taulun konetietoihin.
        "workcenter_id" : to_int64(combined_work_intervals["WorkCenterId"]),
        #Työstettävän nimikkeen tunniste. Yhdistyy 
        "article_id" : to_int64(combined_work_intervals["ManufacturingOrderPartId"]),
        #Koneen käyttäjän tunniste.
        "user_id": to_int64(combined_work_intervals["ReportingEmployeeId"]),
        #Koneessa ajossa olevan ohjelman nimi. 
        "program_name" :pd.NA,
        #Konetapahtuman aloituspäivä semanttisessa avainformaatissa (YYYYMMDD).
        "start_date":  get_date_key(combined_work_intervals["IntervalStart"]),
        #Konetapahtuman päättymispäivä semanttisessa avainformaatissa (YYYYMMDD).
        "end_date":  get_date_key(combined_work_intervals["IntervalEnd"]),
        #Tapahtuman aloitusaika. Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
        "start_time": to_hhmmss(combined_work_intervals["IntervalStart"]),
        #Tapahtuman lopetusaika. Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
        "end_time": to_hhmmss(combined_work_intervals["IntervalEnd"]),
        #Tapahtuman aloituksen aikaleima ISO8601 formaatissa.
        "start_at": combined_work_intervals["IntervalStart"],
        #Tapahtuman lopetuksen aikaleima ISO8601 formaatissa.
        "end_at": combined_work_intervals["IntervalEnd"],
        #Tapahtuman kesto minuutteina. Laskettu aloitusajan ja lopetusajan erotuksesta.
        "duration_min": duration,
        #Yhden kappaleen laskettu kappaleaika. Voidaan laskea esim. jakamalla duration_min / produced_qty.
        "unit_time_min" : handle_unit_time,
        #Yhden kappaleen keskimääräinen sykliaika, jos on saatavilla.  Aika kappaleen valmistumisesta toisen kappaleen valmistumiseen. Usean kappaleen tapauksessa esim. keskiarvo.
        "unit_cycle_time_min" : pd.NA,
        #Yhden kappaleen työstöaika. Aika kappaleen työstön aloituksesta työstön loppumiseen. Useamman kappaleen tapauksessa esim. keskiarvo.
        "unit_processing_time_min" : pd.NA,
        #Tuotettujen kappaleiden määrä tapahtuman aikana.
        "produced_qty_stamping": combined_work_intervals["ReportedQuantity"],
        #Konedatasta laskettu tuotettujen kappaleiden määrä tapahtuman aikana.
        "produced_qty_machine":pd.NA, #UUSI
        #Hylättyjen kappaleiden määrä. Mahdollisesti saatavilla leimausdatassa.
        "rejected_qty_stamping": combined_work_intervals["RejectedQuantity"],
        #Koneen kappalelaskurin arvo tapahtuman loputtua.
        "part_counter" : pd.NA, #UUSI
        #Koneen tehokas työstöaika tapahtuman aikana johdettuna konedatasta.
        "efficient_production_min" : pd.NA,
        #Tehokkaan työstöajan osuus tapahtuman ajasta (effective_production_min/duration_min).
        "efficient_production_pct" : pd.NA,
        #Koneen tehoton työstöaika tapahtuman aikana johdettuna konedatasta. Kone käy mutta ei "täydellä teholla".
        "inefficient_production_min" : pd.NA,
        #Tehottoman työstöajan osuus tapahtuman ajasta (ineffective_production_min/duration_min).
        "inefficient_production_pct" : pd.NA,
        #Koneen varsinainen lastuamisaika aktiivisen työn aikana.
        "cutting_time_min" : pd.NA, #UUSI
        #Aktiivisen työn aikana osuus ajasta, jolloin lastu ei irtoa. Sisältää esim. työstöpäiden pikaliikkeet, työkalujen vaihdot, työstönaikaiset mittaukset jne.
        "non_cutting_time_min" : pd.NA, #UUSI
        #Keskimääräinen syötön prosenttikytkimen (feed rate override) arvo työstön aikana.
        "feed_override" : pd.NA, #UUSI
        #Keskimääräinen karan nopeuden prosenttikytkimen (spindle speed override) arvo työstön aikana.
        "spindle_override": pd.NA,
        #Aktiivisen työstön määrä minuutteina.
        "machine_active_min": pd.NA,
        #Aktiivisen työstöajan osuus tapahtuman ajasta (machine_active_min/duration_min).
        "machine_active_pct": pd.NA,
        #setusajan määrä tapahtuman aikana. On yrityskohtaista mitä asetusaikaan sisällytetään.
        "setup_time_min" : pd.NA,
        #Asetusajan osuus tapahtuman kokonaisajasta.
        "setup_time_pct" : pd.NA,
        #Kappaleen vaihtoajan määrä tapahtuman aikana. Vaihtoaika on aika kappaleen työstön päättymisestä seuraavan kappaleen työstön alkuun saman työvaiheen aikana.
        "part_change_time_min" : pd.NA,
        #Vaihtoajan osuus tapahtuman kokoanaisajasta.
        "part_change_time_pct" : pd.NA,
        #Muun tyhjäkäyntiajan määrä tapahtuman aikana. Käytetään, jos aikaa ei voi määrittää asetusajaksi tai kappaleen vaihtoajaksi.
        "undefined_idle_min" : pd.NA,
        #Muun tyhjäkäynnin osuus tapahtuman kokonaisajasta.
        "undefined_idle_pct": pd.NA,
        #Summattu koneen tyhjäkäynti minuutteina tapahtuman aikana (setup_time_min + part_change_time_min + undefimed_idle_min)
        "idle_sum_min" : pd.NA,
        #Tyhjäkäynnin osuus tapahtuman ajasta (idle_sum_min/duration_min).
        "idle_sum_pct" : pd.NA,
        #Kone poissa päällä minuutteina.
        "power_off_min": pd.NA,
        #Poissaolon aika koko ajasta (power_off_min/duration_min).
        "power_off_pct": pd.NA,
        #Aika jota ei saatu kohdistettua mihinkään tapahtuman aikana. Esimerkiksi erotus duration_min ja eri tila-aikojen summasta.
        "other_min": pd.NA,
        #Kohdistumattoman ajan osuus tapahtuman kokonaisajasta.
        "other_pct" : pd.NA,
        #Poikkeustilan (koneessa esim. virhetila päällä) kesto minuutteina.
        "exception_min": pd.NA,
        #Poikkeustilan osuus koko ajasta (exception_min/duration_min).
        "exception_pct": pd.NA,
        #Poikkeustilan yksilöivä tunniste. Yhdistyy dim_exception tauluun, jossa tarkemmat tiedot. Voidaan käyttää vaikka koneen ilmoittamien virhtilojen seurantaan.
        "exception_id": pd.NA,
        #Tapahtuman aikana kulutettu energiamäärä kWh, jos on saatavilla esim. konedatassa.
        "power_consumption_kwh": pd.NA,
        #Kuvauksia, huomautuksia tai muuta tapahtumaan liittyvää tietoa. Voidaan käyttää myös lisädatan säilömiseen, jos kentät eivät riitä.
        "notes": pd.NA,
    })

    return fact_machine_events


def process_and_stage_fact_order_rows(combined_sales_path: Path):
    print("process_and_stage_fact_order_rows")
    combined_sales = pd.read_parquet(combined_sales_path)

    ordered_qty = pd.to_numeric(combined_sales["OrderedQuantity"],errors="coerce")
    unit_price = pd.to_numeric(combined_sales["Price"],errors="coerce").astype("float")

    new_article = combined_sales["PartStatus"] == 3
    order_complete = combined_sales["CustomerOrderRowStatus"] == 9

    fact_order_rows = pd.DataFrame({
        #Tilausrivin uniikki tunniste. Voi olla sijaisavain tai yhdistelmä tilausnumerosta ja rivistä.
        "order_row_id": to_int64(combined_sales["OrderRowId"]),
        #Tilauksen uniikki tunniste. Tilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "order_number": combined_sales["OrderNumber"],
        #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "work_number": combined_sales["ManufacturingOrderNumber"],
        #Asiakastaulun viiteavain. Yhdistyy dim_customer tauluun.
        "customer_id": to_int64(combined_sales["CustomerId"]),
        #Myyjän tunniste. Voi olla tyhjä. Yhdistyy dim_user dimensiotauluun.
        "seller_id": to_int64(combined_sales["SellerId"]),
        #Nimikkeen/tuotteen tunniste. Yhdistyy dim_article dimensiotauluun.
        "article_id": to_int64(combined_sales["PartId"]),
        #ERP-järjestelmässä tilauksen luontipäivä. Käyttää semanttista avainformaattia (YYYMMDD).
        "order_created_date": get_date_key(combined_sales["OrderRowDate"]),
        #Suunniteltu tuotannon aloituspäivä, jos kyseiseen tilaukseen liittyy tuotantotilaus.
        "production_start_date_planned" : get_date_key(combined_sales["StartDate"]),
        #Tuotannon toteutunut aloituspäivä. Ensimmäisen tuotantovaiheen aikaleimasta.
        "production_start_date_actual" : get_date_key(combined_sales["ActualStartDate"]),
        #Suunniteltu tuotannon päättymispäivä ennen tuotantoa.
        "production_end_date_planned" : get_date_key(combined_sales["InitialFinishDate"]),
        #Tuotannon toteutunut päättymispäivä. Viimeisen tuotantovaiheen aikaleimasta.
        "production_end_date_actual" : get_date_key(combined_sales["ActualFinishDate"]),     
        #Alustava toimituspäivä, joka asetettu tilauksen luonnin tai työn suunnittelun yhteydessä.   
        "delivery_date_planned":  get_date_key(combined_sales["DesiredDeliveryDate"]),
        #Toteutunut toimituspäivä.
        "delivery_date_actual":   get_date_key(combined_sales["DeliveryDate"]),
        #Laskun lähetyksen päivä laskutustiedoista.
        "invoice_date_actual" : get_date_key(combined_sales["InvoiceDate"]),
        #Laskun maksupäivä päivä laskutustiedoista.
        "payment_date_actual" : pd.NA, #UUSI
        #Tilauksen tila. Alustavat vaihtoehdot: PLACED, PROCESSING, COMPLETE, CANCELED (CONFIRMED? PAID? DELIVERED?)
        "order_state_id": pd.to_numeric(combined_sales["ManufacturingOrderStatus"],errors="coerce"),
        #Tilauksen tyyppi. Mainly SALES_ORDER, but can be e.g. "OTHER" or "WARRANTY_WORK".
        "order_type": pd.to_numeric(combined_sales["OrderRowType"],errors="coerce"),
        #Tilattu määrä tuotetta. Oletuksena toimitettu määrä saman verran, joten ei erikseen tietomallissa. Eri työvaiheissa tuotetut määrät fact_workphases taulussa.
        "ordered_qty": ordered_qty,
        #Tilausrivin tuotteen yhden kappaleen hinta.
        "unit_price": unit_price,
        #Tilausrivin arvo valuutassa ilman arvonlisäveroa. Saadaan yleensä kertomalla unit_price * ordered_qty
        "total_price": ordered_qty * unit_price,
        #Suunniteltu suoran työn kustannus valmistuskustannuksissa. On eri tapoja laskea. Voidaan summata eri vaiheiden konetuntihinnoista laskettuja arvoja tai jyvittämällä henkilöiden työn osuutta (jos konekustannukset yleiskustannuksissa).
        "direct_work_cost_planned" : pd.NA,
        #Toteutunut suoran työn kustannus valmistuskustannuksissa.
        "direct_work_cost_actual" : pd.NA, #UUSI
        #Valmistuksen suunnitellut suorat materiaalikulut. Voidaan laskea esim. materiaalien alkuvaraston arvo + ostot - loppuvaraston arvo.
        "material_cost_planned" : pd.NA,
        #Valmistuksen toteutuneet suorat materiaalikulut.
        "material_cost_actual" : pd.NA,
        #Alihankinnan suunnitellut suorat kulut. Voi sisältää myös alihankintaan liittyvät logistiikkakulut
        "subcontracting_cost_planned" : pd.NA,
        #Alihankinnan toteutuneet suorat kulut. Voi sisältää myös alihankintaan liittyvät logistiikkakulut.
        "subcontracting_cost_actual" : pd.NA,
        #Muut muuttuvat kustannukset, kuten rahti, pakkaus, menetelmälisät…
        "other_direct_costs_planned" : pd.NA,
        #Toteutuneet muut muuttuvat kustannukset, kuten rahti, pakkaus, menetelmälisät…
        "other_direct_costs_actual" : pd.NA,
        #Tuotannon suunnitellut suorat tuotantokustannukset (välittömät kustannukset) ilman yleiskustannuslisää eli summattuna suora työ + materiaalit + alihankinta + muut suorat kustannukset.
        "prime_cost_planned_sum":  pd.NA,
        #Tuotannon toteutuneet suorat tuotantokustannukset (välittömät kustannukset) ilman yleiskustannuslisää eli summattuna suora työ + materiaalit + alihankinta + muut suorat kustannukset.
        "prime_cost_actual_sum" : pd.NA, #UUSI
        #Suunnitellut suorat tuotantokustannukset kappaletta kohti.
        "unit_prime_cost_planned": pd.NA,
        #Toteutuneet suorat tuotantokustannukset kappaletta kohti.
        "unit_prime_cost_actual" : pd.NA,
        #Tarjousvaiheessa laskettu myyntikate sisältäen yleiskustannuslisän ja voitto-osuuden. Voidaan laskea esim. total_price - prime_cost_planned_sum
        "sales_margin_planned":   pd.NA,
        #Toteutunut myyntikate sisältäen yleiskustannuslisän ja voitto-osuuden. Voidaan laskea esim. total_price - prime_cost_actual_sum
        "sales_margin_actual" : pd.NA,
        #Hintaan laskettu yleiskustannuslisä eli tilaukselle määritetty  välillisten kustannusten osuus. Yleensä laskettu jonain lisäprosenttina tuotantokustannusten päälle.
        "overhead_costs_planned" : pd.NA,
        #Totetunut yleiskustannuslisä eli tilaukselle määritetty välillisten kustannusten osuus.
        "overhead_costs_actual" : pd.NA, #UUSI
        #Suunnitellut täydet valmistuskustannukset yhteensä. Sisältää suorat tuotantokustannukset ja yleiskustannukset. Synonyyminä usein "omakustannehinta"
        "manufacturing_cost_planned" : pd.NA,
        #Toteutuneet täydet valmistuskustannukset yhteensä. Sisältää suorat tuotantokustannukset ja yleiskustannukset. Synonyyminä usein "omakustannehinta"
        "manufacturing_cost_actual" : pd.NA, #UUSI
        #Suunnitellut valmistuskustannukset kappaletta kohti.
        "unit_manufacturing_cost_planned" : pd.NA,
        #Toteutuneet valmistuskustannukset kappaletta kohti.
        "unit_manufacturing_cost_actual": pd.NA, #UUSI
        #Suunniteltu voittolisä kaikkien kustannusten vähennyksen jälkeen. Lasketaan vähentämällä myyntihinnasta kaikki kulut. Lasketaan yleensä jonain prosenttina omakustannehinnan päälle.
        "profit_planned" : pd.NA,
        #Toteutunut voittolisä kaikkien kustannusten vähennyksen jälkeen. Lasketaan vähentämällä myyntihinnasta kaikki kulut. Lasketaan yleensä jonain prosenttina
        "profit_actual" : pd.NA, #UUSI
        #Tilattu yksikkö.
        "unit": to_int64(combined_sales["UnitId"]),
        #Käytetty valuutta.
        "currency": "EUR",
        #Tieto siitä onko kyseessä uusi nimike.
        "new_article": new_article,
        #Tilaus on hoidettu ja laskutettu (ei välttämättä maksettu).
        "order_complete": order_complete,
    })

    return fact_order_rows


def process_and_stage_fact_workphases(combined_manufacturing_path : Path):
    print("process_and_stage_fact_workphases")
    combined_manufacturing = pd.read_parquet(combined_manufacturing_path)
   
    planned_unit_time_min = ticks_to_minutes(combined_manufacturing["PlannedUnitTime"])
    planned_setup_time_min = ticks_to_minutes(combined_manufacturing["PlannedSetupTime"])
       
    phase_duration_planned_min = planned_unit_time_min + planned_setup_time_min
   
    reported_unit_time_min = ticks_to_minutes(combined_manufacturing["ReportedUnitTime"])
    reported_setup_time_min = ticks_to_minutes(combined_manufacturing["ReportedSetupTime"])
       
    phase_duration_stamping_min = reported_unit_time_min + reported_setup_time_min
   
    planned_qty = pd.to_numeric(combined_manufacturing["PlannedQuantity"],errors="coerce")
   
    produced_qty = pd.to_numeric(combined_manufacturing["ReportedQuantity"],errors="coerce")
   
    rejected_qty = pd.to_numeric(combined_manufacturing["RejectedQuantity"],errors="coerce")
   
    actual_finish = combined_manufacturing["ActualFinishDate"]
    phase_duration_per_piece_min = (
        pd.Series(phase_duration_stamping_min / produced_qty)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )
   
    actual_start_derived = actual_finish - pd.to_timedelta(
           phase_duration_per_piece_min,
           unit="m"
       )
   
    start_timestamp_actual = get_date_key(actual_start_derived)
   
    start_timestamp_actual = pd.Series(start_timestamp_actual).fillna(
           get_date_key(combined_manufacturing["ActualStartDate"])
       )
   
    fact_workphases = pd.DataFrame({
           #Työvaiherivin yksilöivä uniikki avain.
           "workphase_id": to_int64(combined_manufacturing["ManufacturingOrderOperationId"]),
           #Liittää työvaiheen tilausrvitauluun. Monesta-yhteen tyyppinen yhteys.
           "order_row_id": to_int64(combined_manufacturing["CustomerOrderRowId"]),
           #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
           "work_number": combined_manufacturing["OrderNumber"],
           #Työtilauksen tunniste. Ei ole erikseen dimensiotaulua.
           "work_id" : to_int64(combined_manufacturing["ManufacturingOrderId"]),
           #Yksittäisen koneen/kuormitusryhmän/muun työkeskuksen tunniste (koneet, alihankkijat, muut työvaiheet). Tässä tietomallissa erilaiset työkeskukset tulevat samalla tavalla. 
           #Yhdistyy dim_workcenter ulottuvuustauluun. Vaatii tarkempaa määritystä mm. tilanteessa, jossa työ on toteutettu eri koneella kuin alkujaan suunniteltu tai työvaihe on jaettu usean koneen kesken.
           "workcenter_id" : to_int64(combined_manufacturing["WorkCenterId"]),
           #Työkeskuksen tyypin tunniste. Yhdistyy dim_workcenter_group tauluun. 
           "workcenter_group_id" : pd.NA, #UUSI
           #Nimikkeen/tuotteen tunniste. Yhdistyy dim_article ulottuvuustauluun.
           "article_id": to_int64(combined_manufacturing["OperationPartId"]),
           #Työvaiheen numero. Tyypillisesti järjestyksessä, jos useampi vaihe työssä.
           "phase_number" : combined_manufacturing["OperationNumber"],
           #Työvaiheen kuvaus, jos saatavilla. Voidaan käyttää erottamaan eri tyyppisiä työvaiheita toisistaan (työstö, alihankinta, kokoonpano…)
           "phase_description" : combined_manufacturing["Description_x"],
           #Tieto siitä, onko kyseessä alihankintatyövaihe
           "subcontracting_phase" : combined_manufacturing["IsSubcontractor"],
           #Työvaiheen alkuperäinen suunniteltu aloituspäivä ERP/MES järjestelmästä.
           "start_date_original_planned" : pd.NA,  ##TARKISTA
           #Työvaiheen viimeisin suunniteltu aloituspäivä ERP/MES järjestelmästä. Ensimmäinen ajankohta, jos on pilkottu useaan osaan vaihe.
           "start_date_latest_planned" : get_date_key(combined_manufacturing["PlannedStartDate"]),
           #Työvaiheen toteutunut aloituspäivä (yleensä leimausdatasta).
           "start_date_actual": get_date_key(combined_manufacturing["ActualStartDate"]),
           #Työvaiheen alkuperäinen suunniteltu lopetuspäivä ERP/MES järjestelmästä.
           "end_date_original_planned": pd.NA, ##TARKISTA
           #Työvaiheen viimeisin suunniteltu lopetuspäivä ERP/MES järjestelmästä. Viimeinen ajankohta, jos on pilkottu useaan osaan vaihe.
           "end_date_latest_planned": get_date_key(combined_manufacturing["PlannedFinishDate"]),
           #Työvaiheen toteutunut lopetuspäivä (yleensä leimausdatasta).
           "end_date_actual": get_date_key(combined_manufacturing["ActualFinishDate"]),
           #Työvaiheen viimeisin suunniteltu aloitusaika ERP/MES järjestelmästä (jos saatavilla). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "start_time_planned": pd.NA,
           #Työvaiheen toteutunut aloitusaika (yleensä leimaustiedoista). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "start_time_actual": to_hhmmss(combined_manufacturing["ActualStartDate"]),
           #Työvaiheen viimeisin suunniteltu lopetusaika ERP/MES järjestelmästä (jos saatavilla). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "end_time_planned": pd.NA,
           #Työvaiheen toteutunut lopetusaika (yleensä leimaustiedoista). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "end_time_actual": to_hhmmss(combined_manufacturing["ActualFinishDate"]),
           #Työvaiheen suunnitellun aloitusajan aikaleima ISO8601-formaatissa. 
           "start_timestamp_planned": combined_manufacturing["PlannedStartDate"],
           #Työvaiheen suunnitellun lopetusajan aikaleima ISO8601-formaatissa. 
           "end_timestamp_planned": combined_manufacturing["PlannedFinishDate"],
           #Työvaiheen toteutuneen aloitusajan aikaleima ISO8601-formaatissa. 
           "start_timestamp_actual": combined_manufacturing["ActualStartDate"],
           #Työvaiheen toteutuneen lopetusajan aikaleima ISO8601-formaatissa.
           "end_timestamp_actual" : combined_manufacturing["ActualFinishDate"],
           #Vaiheessa tuotettavaksi suunniteltujen kappaleiden määrä. Huom! Voi olla eri määrä kuin yksittäisellä tilausrivillä.
           "planned_qty": planned_qty,
           #Vaiheessa kaikkiaan tuotettu kappalemäärä.
           "produced_qty": produced_qty,
           #Hylättyjen kappaleiden määrä.
           "rejected_qty": rejected_qty,
           #Työkeskuksen tuntihinta. Yleensä käytännössä koneelle määritetty konetuntihinta kyseisen työn ajankohtana.
           "workcenter_hour_rate" : pd.NA,
           #Työvaiheen suunnitellut suoran työn kustannukset. Voi olla laskettu myös workcenter_hour_rate pohjalta, jos se on kustannusmallina.
           "phase_cost_planned" : pd.NA,
           #Työvaiheen toteutuneet suoran työn kustannukset leimausten perusteella. Voi olla laskettu myös konetuntihinnan pohjalta suhteutettuna toteutuneeseen aikaan, jos se on kustannusmallina.
           "phase_cost_actual_stamping" : pd.NA,
           #Työvaiheen toteutuneet suoran työn kustannukset. Voi olla laskettu myös konetuntihinnan pohjalta suhteutettuna toteutuneeseen aikaan, jos se on kustannusmallina.
           "phase_cost_actual_machine" : pd.NA,
           #Suunniteltu asetustyön kesto minuuteissa.
           "setup_time_planned_min": planned_setup_time_min,
           #Toteutunut asetustyön kesto minuuteissa leimausdatan pohjalta (jos saatavilla)
           "setup_time_stamping_min": reported_setup_time_min,
           #Toteutunut asetustyön kesto minuuteissa konedatan pohjalta (jos saatavilla)
           "setup_time_machine_min": pd.NA,
           #Vaiheen suunniteltu työstöaika minuutteina.
           "production_time_planned_min": planned_unit_time_min,
           #Vaiheen toteutunut työstöaika leimausdatan pohjalta.
           "production_time_stamping_min": reported_unit_time_min,
           #Vaiheen toteutunut työstöaika konedatan pohjalta.
           "production_time_machine_min": pd.NA,
           #Suunniteltu yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan.
           "phase_duration_planned_min": phase_duration_planned_min,
           #Toteutunut leimauksista yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan. Kaikkien erillisten työstökertojen summa.
           "phase_duration_stamping_min": ticks_to_minutes(combined_manufacturing["ReportedUnitTime"]),
           #Toteutunut konedatasta yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan. Kaikkien erillisten työstökertojen summa.
           "phase_duration_machine_min": pd.NA,
           #Yhden kappaleen suunniteltu kappaleaika. Saadaan jakamalla suunniteltu vaiheaika (phase_duration_planned_min) suunniteltujen kappaleiden määrällä.
           "unit_time_planned_min" : phase_duration_planned_min / planned_qty,
           #Yhden kappaleen leimausten perusteella kappaleaika. Saadaan jakamalla leimattu vaiheaika tuotettujen kappaleiden määrällä.
           "unit_time_stamping_min" : reported_unit_time_min / produced_qty,
           #Yhden kappaleen konedatan perusteella kappaleaika. Saadaan jakamalla laskettu vaiheaika tuotettujen kappaleiden määrällä.
           "unit_time_machine_min" : pd.NA,
           #Yhden kappaleen paras mahdollinen tavoiteltu sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä. Voi olla eri kuin mitä suunnittelussa käytetään. Käytetään OEE laskennassa, jos saatavilla.
           "cycle_time_ideal_min" : pd.NA,
           #Yhden kappaleen suunniteltu sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä.
           "cycle_time_planned_min" : planned_unit_time_min / planned_qty,
           #Yhden kappaleen leimausten perusteella sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä. Paras arvio, jos on erikseen mitattu ja leimattu.
           "cycle_time_stamping_min" : pd.NA,
           #Yhden kappaleen konedatan perusteella laskettu sykliaika/jaksoaika. Laskettu keskimääräinen tai mediaaniaika kappaleen valmistumisesta seuraavan kappaleen valmistumiseen.
           "cycle_time_machine_min" : pd.NA,
           #Yhden kappaleen tavoiteltu koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen. Voi olla eri asia kuin suunniteltu tai toteutunut koneistusaika.
           "processing_time_ideal_min" : pd.NA,
           #Yhden kappaleen suunniteltu koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_planned_min" : pd.NA,
           #Leimausdatan perusteella yhden kappaleen koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_stamping_min" : pd.NA,
           #Konedatan perusteella yhden kappaleen koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_machine_min" : pd.NA
       })

    return fact_workphases
    
def process_dim(staged_path: Optional[Path] = None,combined_path: Optional[Path] = None):
    print("process_dim")
    combined_path = combined_path or COMBINED_PATH
    staged_path = staged_path or STAGED_PATH
    _, work_centers = load_latest(staged_path / "manufacturing", "WorkCenters",LoadMode.STAGED)
    _, customers = load_latest(staged_path / "sales", "Customers",LoadMode.STAGED)
    _, order_types = load_latest(staged_path / "sales", "CustomerOrderTypes",LoadMode.STAGED)
    _, parts = load_latest(combined_path / "inventory", "CombinedInventory",LoadMode.COMBINED)

    rename_work_centers = {
        "Id" : "workcenter_id",
        "Name" : "workcenter_description",
        "Type" : "workcenter_type",
        "Description" : "workcenter_name", 
    }

    work_center_int_cols = ["workcenter_id"]
    work_center_keep_columns = [ "workcenter_id","workcenter_name","workcenter_description", "workcenter_type"]
    work_centers = shape_and_types(work_centers,rename_work_centers,work_center_keep_columns,work_center_int_cols)


    rename_parts = {
        "Id_part" : "article_id",
        "Code" : "article_code",
        "PartNumber" : "article_name",
        "Alias_part" : "article_name_other",
        "Description_part" : "article_description",
        "ActiveRevisionId" : "revision",
        "ExtraDescription" : "notes",
    }

    part_int_columns = ["article_id","revision"]
    part_float_columns = ["prime_cost"]
    part_keep_columns = [ "article_id","article_code","article_name","article_name_other","article_description","revision","notes"]
    parts = shape_and_types(parts,rename_parts,part_keep_columns,part_int_columns,part_float_columns)

    rename_customers = {
        "Id" : "customer_id",
        "Code" : "customer_number",
        "Name" : "customer_name",
    }
    

    customer_int_columns = ["customer_id"]
    customer_keep_columns = ["customer_id","customer_number", "customer_name","location"]
    customers = shape_and_types(customers,rename_customers,customer_keep_columns,customer_int_columns)


    rename_order_types= {
        "Id" : "order_type_id",
        "Description" : "order_type_name",
    }
    
    order_type_int_columns = ["order_type_id"]
    order_types_to_keep = [ "order_type_id","order_type_name"]
    order_types = shape_and_types(order_types,rename_order_types,order_types_to_keep,order_type_int_columns)

    return {
        "dim_workcenter": work_centers,
        "dim_article": parts,
        "dim_customer": customers,
        "dim_order_type": order_types,
    }
   
    

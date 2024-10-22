# -*- coding: utf-8 -*-
# Copyright (c) 2021, itsdave and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
from os import error
import frappe
from pprint import pprint
from frappe.utils import file_manager
from frappe.model.document import Document
from datetime import datetime
import pandas as pd
import calendar
from erpnext.accounts.party import set_taxes as party_st
from collections import Counter

class ADNImport(Document):
    log_list = []
    settings_doc = frappe.get_single("ADN Import Settings")

    @frappe.whitelist()
    def do_import(self):
        
        log_list = self.log_list 
        #gezählt werden vollstädig erstellte und vorbereitete Rechnungen
        count_erfolgreich_erstellte_rechnung = 0
        rechnungen = self.get_invoice_dict_from_csv()
        lizenzen = 0
        betrag_ausgangsrechnungen = 0
        adn_rechnungsbetrag = 0
        for rechnung in rechnungen:
            
            #gezählt wird die Gesamtzahl der Lizenzen
            for position in rechnung["positionen"]:
                betrag_ausgangsrechnungen += (float(position['preis'])*int(position['menge']))
                adn_rechnungsbetrag += (float(position['positionspreis']))
                if float(position["preis"]) > 0: 
                    lizenzen += int(position["menge"])
          
            log_eintrag_vorhanden = False
            if self.check_adn_invoice_number(rechnung):
            
                customer_exists = self.check_erpn_customer(rechnung["kdnr"])
                                    
                if not customer_exists:
                    #wenn die csv-Datei keine Endkundenreferenz enthält, 
                    # Rechnung für default_customer vorbereiten
                    rechnung["kdnr"] = self.settings_doc.default_customer
                    log_list.append("ADN-Rechnung " + str(rechnung["adn_rg"]) + " für Kunden " + str(rechnung["kunde"]) + " ist vorbereitet, kann aber nicht eindeutig zugeordnet werden")
                    log_eintrag_vorhanden = True

                return_dict = self.create_erpn_invoice(rechnung)
                if return_dict["status"]:
                    count_erfolgreich_erstellte_rechnung += 1
                else:
                    log_list.append("Rechnung mit ADN-Rechnungs-Nr.: " + str(rechnung["adn_rg"]) + " konnte nicht erstellt werden:")
                    log_eintrag_vorhanden = True
                    for i in return_dict["fehler"]:
                        log_list.append(i)

                if log_eintrag_vorhanden:
                    self.status = "fehlerhaft"
                    self.save()
                    log_list.append("#############################")
                else:
                    self.status = "erfolgreich"
                    self.save()
        
        rechnungsdatum = datetime.strptime(rechnung["datum"],"%d.%m.%Y %H:%M:%S")   
        self.rechnungsdatum = datetime.strftime(rechnungsdatum, "%m.%Y")
        self.anzahl_der_lizenzen = lizenzen
        self.betrag_ausgangrechnungen = round(betrag_ausgangsrechnungen,2)
        self.adn_rechnungsbetrag = round(adn_rechnungsbetrag,2)
        self.rohertrag = round(betrag_ausgangsrechnungen - adn_rechnungsbetrag,2)
           
        log_list.append(str(count_erfolgreich_erstellte_rechnung) + " Rechnungen wurden erstellt")
        log_str = ""
        for i in log_list:
            log_str += i + "\n"
               
        self.log = log_str
        self.save()

    def erzeuge_gs_wenn_erforderlich(self, rechnungen):
        #Kenntlich machen von Gutschriften
        #Rechnungen die sowohl Gutschriften als auch Rechnugsposten enthalten werden aufgeteilt.
        
        rechnungen_mit_gs_list = []

        for rechnung in rechnungen:
            if rechnung["gs_erforderlich"]:
                gs_dict = rechnung.copy()
                rg_dict = rechnung.copy()
                
                rg_positionen_neu_list = [] 
                gs_positionen_neu_list = []

                for pos in rechnung["positionen"]:
                    if pos["preis"] < 0:
                        gs_positionen_neu_list.append(pos)
                    else:
                        rg_positionen_neu_list.append(pos)
                       
                gs_dict["positionen"] = gs_positionen_neu_list                
                rg_dict["positionen"] = rg_positionen_neu_list

                gs_dict["adn_rg"] = "GS-" + str(gs_dict["adn_rg"])+str(gs_dict["kdnr"])
                rg_dict["gs_erforderlich"] = False
                
                if len(gs_dict["positionen"]) > 0:
                    rechnungen_mit_gs_list.append(gs_dict)
                    
                if len(rg_dict["positionen"]) > 0:   
                    rechnungen_mit_gs_list.append(rg_dict)
                
            else:
                rechnungen_mit_gs_list.append(rechnung)
        return rechnungen_mit_gs_list

    def get_invoice_dict_from_csv(self):   
        csv_file = frappe.utils.file_manager.get_file_path(self.datei)
        rechnungen = []
        rechnung ={}
        df = pd.read_csv(csv_file, encoding='utf-8-sig', delimiter=";", decimal=",") 
        
        # df['Wartungsbeginn'] = pd.to_datetime(df['Wartungsbeginn'])
        # df['Wartungsende'] = pd.to_datetime(df['Wartungsende'])
        df = df.fillna(0)
        
        
        kunde= ""
        erste_zeile = list(df.columns)
        print(erste_zeile)
        
        if self.validate_csv(erste_zeile):

            for row, pos in df.iterrows():
                               
                if pos['Endkunde'] != kunde:
                                            
                    if kunde != "":
                        rechnungen.append(rechnung)
                                        
                    kunde = pos['Endkunde']
                
                    rechnung = {}
                    #Beginn neuer Rechnung, Kopfdaten auslesen
                    rechnung["kdnr"] =  pos['Endkunde_Reference']
                    rechnung["adn_rg"] = str(pos['RECHNUNG'])+'-' +str(pos['Endkunde_Reference'])+'-' +str(pos['Endkunde'])
                    rechnung["kunde"] = pos['Endkunde']
                    rechnung["art"] = pos['Rechnungsart']
                    rechnung["datum"] = pos['DATUM']
                    rechnung["positionen"] = []
                    rechnung["gs_erforderlich"] = False   
                else:
                    #für jede weitere Zeile einer Rechnung erkennen wir
                    #die weiteren Positionen
                    von_dt = datetime.strptime (str(pos['Wartungsbeginn']),"%d.%m.%Y %H:%M:%S")
                    bis_dt = datetime.strptime (str(pos['Wartungsende']),"%d.%m.%Y %H:%M:%S")
                    # von_dt = pos['Wartungsbeginn']
                    # bis_dt = pos['Wartungsende']
                    print(von_dt, bis_dt)
                    time_delta = bis_dt - von_dt 
                                    
                    position = {"artikel": pos['HERSTELLERNUMMER'],
                            "von": von_dt,
                            "bis": bis_dt,  
                            "menge": pos['MENGE'], 
                            "preis": pos['Listpreis'],
                            "positionspreis": pos['Positionspreis'],
                            "wartungsdauer": time_delta.days+1,
                            "gesamtdauer": calendar.monthrange(von_dt.year,von_dt.month)[1],
                            "vertrag" : pos['Vertrag']
                            }
                    if float(pos['Listpreis'])<0:
                        rechnung["gs_erforderlich"] = True
                                            
                    rechnung["positionen"].append(position)
                    
            rechnungen.append(rechnung)
            rechnungen = self.erzeuge_gs_wenn_erforderlich(rechnungen)        
            return(rechnungen)
        else:
            frappe.msgprint("ACHTUNG Rechnungen konnten nicht erstellt werden. CSV- Format stimmt nicht mit dem Standartformat überein")
                    
        
    def validate_csv(self, erste_zeile):
        # Prüfen, ob die csv-Datei das erwartete Format aufweist
         
        soll_header = ['RECHNUNG', 'DATUM', 'KUNDENNR', 'DEBITORKONTO', 'SACHBEARBEITER',
       'LIEFERSCHEIN', 'LIEFERSCHEINDATUM', 'UStID', 'RE_FIRMA', 'RE_ADRESSE',
       'RE_PLZ', 'RE_ORT', 'RE_LAND', 'LI_FIRMA', 'LI_STRASSE', 'LI_PLZ',
       'LI_ORT', 'LI_LAND', 'HA_FIRMA', 'HA_STRASSE', 'HA_PLZ', 'HA_ORT',
       'HA_LAND', 'Warenwert', 'MWst', 'Gesamtbetrag', 'NettoZahlbarBis',
       'Lieferbedingungen', 'Zahlungsbedingung', 'Endkunde', 'POSITION',
       'ARTIKEL', 'HERSTELLERNUMMER', 'ARTIKELBEZ', 'MENGE', 'PREISME',
       'Listpreis', 'Rabatt', 'Einzelpreis', 'Positionspreis',
       'Wartungsbeginn', 'Wartungsende', 'Vertrag', 'Marketplace_Ref',
       'Order_Reference', 'Endkunde_Reference', 'Subscription_ID_External',
       'Subscription_Start_Date', 'Buchungstyp', 'OrderDatum', 'Rechnungsart',
       'MSERP', 'MSERP_BillingPeriod','BillingPlan', 'VertragsDauer']
        
        
        if soll_header == erste_zeile:
            self.log_list.append("CSV wurde erfolgreich überprüft")
            return True

        else:
            if self.zustimmung == 1:
                self.log_list.append("CSV Format entspricht zwar nicht dem Standartformat, der Import wurde trotzdem durchgeführt")
                return True
            else:
                self.log_list.append("CSV Format entspricht nicht dem Standartformat")
                return False  
       
    
    def check_adn_invoice_number(self,rechnung):
        ausgangs_rechnungen =  frappe.get_all("Sales Invoice", filters={"adn_invoice_number": str(rechnung["adn_rg"]) })
        if len(ausgangs_rechnungen) == 0:
            return True
        else:
            self.log_list.append("Für den Kunden: " + rechnung["kunde"] + " existiert bereits eine Rechnung, es wurde keine neue Rechnung erstellt")      
            return False



    # def create_erpn_invoice(self, rechnungen):
    #     return_dict = {"status": False, "fehler": []}
    #     rechnung_doc = frappe.get_doc({
    #            "doctype": "Sales Invoice"
    #     }) 
            
    #     rechnungsdatum = datetime.strptime(rechnungen["datum"], "%d.%m.%Y %H:%M:%S")
    #     rechnungsmonat = datetime.strftime(rechnungsdatum, "%m.%Y")

    #     if rechnungen["gs_erforderlich"] == True:
    #         rechnung_doc.is_return = 1
    #         rechnung_doc.title = str(self.settings_doc.title_and_prefix_credit_note) + " " + rechnungsmonat
    #     else:
    #         rechnung_doc.title = str(self.settings_doc.title_and_prefix) + " " + rechnungsmonat

    #     rechnung_doc.customer = rechnungen["kdnr"]
    #     rechnung_doc.introduction_text = self.settings_doc.introduction_text
    #     rechnung_doc.company = self.settings_doc.company
    #     customer_doc = frappe.get_doc("Customer", rechnungen["kdnr"])

    #     if customer_doc.payment_terms:
    #         rechnung_doc.payment_terms_template = customer_doc.payment_terms
    #     else:
    #         rechnung_doc.payment_terms_template = self.settings_doc.payment_terms_template
    #     rechnung_doc.tc_name = self.settings_doc.tc_name

    #     tac_doc = frappe.get_doc("Terms and Conditions", self.settings_doc.tc_name)
    #     rechnung_doc.terms = tac_doc.terms

    #     rechnung_doc.taxes_and_charges = party_st(rechnung_doc.customer, "Customer", rechnung_doc.posting_date, rechnung_doc.company)
    #     taxes = frappe.get_doc("Sales Taxes and Charges Template", rechnung_doc.taxes_and_charges).taxes

    #     for tax in taxes:
    #         new_tax = frappe.get_doc({
    #             "doctype": "Sales Taxes and Charges",
    #             "charge_type": tax.charge_type,
    #             "account_head": tax.account_head,
    #             "rate": tax.rate,
    #             "description": tax.description
    #         })
    #         rechnung_doc.append("taxes", new_tax)

    #     rechnung_doc.adn_invoice_number = rechnungen["adn_rg"]
    #     artikel_zaehler = Counter([position["artikel"] for position in rechnungen["positionen"]])

    #     if rechnungen["gs_erforderlich"] == True:
    #         for position in rechnungen["positionen"]:
    #             menge = -position["menge"]
    #             preis_csv = -position["preis"]
    #             preis = preis_csv / 0.825
    #             rechnung_doc_artikel = frappe.get_doc({
    #                 "doctype": "Sales Invoice Item",
    #                 "item_code": position["artikel"],
    #                 "qty": menge,
    #                 "rate": preis
    #             })
    #             rechnung_doc.append("items", rechnung_doc_artikel)

    #     else:
    #     # Überprüfen, ob der Kunde ADN Included Services hat
    #         included_service = frappe.get_all("ADN Included Services", filters={
    #             "customer": rechnung_doc.customer
    #         })

    #         # Gesamte inkludierte Menge aus ADN Included Services für den Artikel
    #         if included_service:
    #             verbleibende_inklusiv_menge = self.get_inkludierte_menge(included_service)  # Funktion zur Abfrage der inkludierten Menge
    #         else:
    #             verbleibende_inklusiv_menge = 0

    #         for artikel, anzahl in artikel_zaehler.items():
    #             # Falls der Artikel mehr als einmal vorkommt, angepasste Logik verwenden
    #             if anzahl > 1:
    #                 self.handle_multiple_positions(artikel, rechnungen, verbleibende_inklusiv_menge, rechnung_doc)
    #             else:
    #                 # Standardlogik für Artikel, die nur einmal vorkommen
    #                 self.handle_single_position(artikel, rechnungen, verbleibende_inklusiv_menge, rechnung_doc)

    #     return_dict["status"] = True
    #     return return_dict


    # # Methode zur Handhabung von mehrfach vorkommenden Artikeln
    # def handle_multiple_positions(self, artikel, rechnungen, verbleibende_inklusiv_menge, rechnung_doc):
    #     # Summe der Mengen aller Vorkommen des Artikels berechnen
    #     gesamtmenge = sum([position["menge"] for position in rechnungen["positionen"] if position["artikel"] == artikel])

    #     for position in rechnungen["positionen"]:
    #         if position["artikel"] == artikel:
    #             menge = position["menge"]
    #             preis = position["preis"]
    #             inkludierte_menge = 0

    #             # Inkludierte Menge abziehen, falls vorhanden
    #             if verbleibende_inklusiv_menge > 0:
    #                 inkludierte_menge = min(menge, verbleibende_inklusiv_menge)
    #                 self.add_invoice_item(rechnung_doc, artikel, inkludierte_menge, 0, "ohne Berechnung")
    #                 verbleibende_inklusiv_menge -= inkludierte_menge

    #             # Restliche Menge regulär verrechnen
    #             restliche_menge = menge - inkludierte_menge
    #             if restliche_menge > 0:
    #                 self.add_invoice_item(rechnung_doc, artikel, restliche_menge, preis)


    # # Methode zur Handhabung von Einzelartikeln
    # def handle_single_position(self, artikel, rechnungen, verbleibende_inklusiv_menge, rechnung_doc):
    #     for position in rechnungen["positionen"]:
    #         if position["artikel"] == artikel:
    #             menge = position["menge"]
    #             preis = position["preis"]
    #             inkludierte_menge = 0

    #             # Inkludierte Menge abziehen, falls vorhanden
    #             if verbleibende_inklusiv_menge > 0:
    #                 inkludierte_menge = min(menge, verbleibende_inklusiv_menge)
    #                 self.add_invoice_item(rechnung_doc, artikel, inkludierte_menge, 0, "ohne Berechnung")
    #                 verbleibende_inklusiv_menge -= inkludierte_menge

    #             # Restliche Menge regulär verrechnen
    #             restliche_menge = menge - inkludierte_menge
    #             if restliche_menge > 0:
    #                 self.add_invoice_item(rechnung_doc, artikel, restliche_menge, preis)


    # # Methode zum Hinzufügen von Rechnungspositionen
    # def add_invoice_item(self, rechnung_doc, artikel, menge, preis, beschreibung=""):
    #     rechnung_doc_artikel = frappe.get_doc({
    #         "doctype": "Sales Invoice Item",
    #         "item_code": artikel,
    #         "qty": menge,
    #         "rate": preis
    #     })
    #     if beschreibung:
    #         rechnung_doc_artikel.description = (rechnung_doc_artikel.get("description") or "") + f"<p><strong>{beschreibung}</strong></p>"
        
    #     rechnung_doc.append("items", rechnung_doc_artikel)

    # def get_inkludierte_menge(self, included_service):
    #     if included_service:
    #         items = frappe.get_all("ADN Included Services Item", filters={
    #             "parent": included_service[0].name,
    #             "item_code": artikel_doc.name
    #         }, fields=["qty"])

    #         if items:
    #             gesamt_inklusiv_menge = items[0].qty 
    #         else:
    #             gesamt_inklusiv_menge = 0
    #     else:
    #         gesamt_inklusiv_menge = 0
    #     return gesamt_inklusive_menge

   

    def check_erpn_customer(self, kundennummer):    
        if kundennummer == 0:
            return False
        else:     
            kunden_liste = frappe.get_all("Customer", filters={"name":kundennummer })
            
            if len(kunden_liste) == 1:
                return True

    def create_rechnung_doc(self, rechnungen):
        rechnung_doc = frappe.get_doc({
            "doctype": "Sales Invoice"
        })

        rechnungsdatum = datetime.strptime(rechnungen["datum"], "%d.%m.%Y %H:%M:%S")
        rechnungsmonat = datetime.strftime(rechnungsdatum, "%m.%Y")

        if rechnungen["gs_erforderlich"]:
            rechnung_doc.is_return = 1
            rechnung_doc.title = f"{self.settings_doc.title_and_prefix_credit_note} {rechnungsmonat}"
        else:
            rechnung_doc.title = f"{self.settings_doc.title_and_prefix} {rechnungsmonat}"

        rechnung_doc.customer = rechnungen["kdnr"]
        rechnung_doc.introduction_text = self.settings_doc.introduction_text
        rechnung_doc.company = self.settings_doc.company
        customer_doc = frappe.get_doc("Customer", rechnungen["kdnr"])

        rechnung_doc.payment_terms_template = customer_doc.payment_terms or self.settings_doc.payment_terms_template
        rechnung_doc.tc_name = self.settings_doc.tc_name

        tac_doc = frappe.get_doc("Terms and Conditions", self.settings_doc.tc_name)
        rechnung_doc.terms = tac_doc.terms

        rechnung_doc.taxes_and_charges = party_st(rechnung_doc.customer, "Customer", rechnung_doc.posting_date, rechnung_doc.company)
        taxes = frappe.get_doc("Sales Taxes and Charges Template", rechnung_doc.taxes_and_charges).taxes

        for tax in taxes:
            new_tax = frappe.get_doc({
                "doctype": "Sales Taxes and Charges",
                "charge_type": tax.charge_type,
                "account_head": tax.account_head,
                "rate": tax.rate,
                "description": tax.description
            })
            rechnung_doc.append("taxes", new_tax)

        rechnung_doc.adn_invoice_number = rechnungen["adn_rg"]

        return rechnung_doc

    def create_erpn_invoice(self, rechnungen):
        return_dict = {"status": False, "fehler": []}
        
        # Erstellen des Rechnungsdokuments
        rechnung_doc = self.create_rechnung_doc(rechnungen)

        for position in rechnungen["positionen"]:
            artikel_liste = frappe.get_all("Item", filters={"hersteller_artikel_nummer": position["artikel"]})

            if len(artikel_liste) == 1:
                artikel_doc = frappe.get_doc("Item", artikel_liste[0]["name"])

                if rechnungen["gs_erforderlich"] == False:
                    included_service = frappe.get_all("ADN Included Services", filters={"customer": rechnung_doc.customer})
                    inkludierte_menge = 0
                    inc_serv_beschreibung = ""  # Initialisierung der inc_serv-Beschreibung

                    if included_service:
                        included_service_doc = frappe.get_doc("ADN Included Services", included_service[0].name)
                        items = frappe.get_all("ADN Included Services Item", filters={
                            "parent": included_service_doc.name,
                            "item_code": artikel_doc.name
                        }, fields=["qty"])

                        if items:
                            inkludierte_menge = items[0].qty

                        # Übernehme die Beschreibung aus dem inc_serv-Feld
                        inc_serv_beschreibung = included_service_doc.inc_serv or ""

                    menge = position["menge"]
                    preis = position["preis"] if self.preisentnahme_aus == "CSV Datei" else ""

                    # 1. Position für Inklusivleistung (mit 100% Rabatt)
                    if inkludierte_menge > 0:
                        self.add_item_to_invoice(
                            rechnung_doc,
                            artikel_doc.name,
                            min(menge, inkludierte_menge),
                            preis,
                            rabatt=100,
                            von=position["von"],
                            bis=position["bis"],
                            vertrag=position["vertrag"],
                            beschreibung=inc_serv_beschreibung
                        )

                    # 2. Restliche Position (mit regulärem Preis)
                    restliche_menge = menge - inkludierte_menge
                    if restliche_menge > 0:
                        self.add_item_to_invoice(
                            rechnung_doc,
                            artikel_doc.name,
                            restliche_menge,
                            preis,
                            von=position["von"],
                            bis=position["bis"],
                            vertrag=position["vertrag"]
                        )

                    # Normale Position, wenn keine Inklusivleistung vorhanden
                    if inkludierte_menge == 0:
                        self.add_item_to_invoice(
                            rechnung_doc,
                            artikel_doc.name,
                            menge,
                            preis,
                            von=position["von"],
                            bis=position["bis"],
                            vertrag=position["vertrag"]
                        )

                else:  # Wenn gs_erforderlich == True
                    menge = -position["menge"]
                    preis_csv = -position["preis"]
                    preis = preis_csv / 0.825
                    self.add_item_to_invoice(
                        rechnung_doc,
                        artikel_doc.name,
                        menge,
                        preis,
                        von=position["von"],
                        bis=position["bis"],
                        vertrag=position["vertrag"]
                    )

            else:
                if len(artikel_liste) == 0:
                    meldung = f"Zu Herstellerartikelnummer {position['artikel']} konnte kein Artikel gefunden werden"
                    return_dict["fehler"].append(meldung)
                else:
                    meldung = f"Zu Herstellerartikelnummer {position['artikel']} wurden mehrere Artikel gefunden, daher konnte keine eindeutige Zuordnung erfolgen"
                    return_dict["fehler"].append(meldung)

        if not return_dict["fehler"]:
            rechnung_doc.save()
            rechnung_doc.set_taxes()
            rechnung_doc.save()
            rg_nr = rechnung_doc.name
            return_dict["status"] = rg_nr

        return return_dict



    def add_item_to_invoice(self, rechnung_doc, artikel_code, menge, preis, rabatt=0, von=None, bis=None, vertrag=None, beschreibung=None):
        """Fügt einen Artikel zu einem Rechnungsdokument hinzu und erstellt die Beschreibung.

        Args:
            rechnung_doc: Das Sales Invoice Dokument, zu dem der Artikel hinzugefügt wird.
            artikel_code: Der Artikelcode (Item).
            menge: Die Menge des Artikels.
            preis: Der Preis pro Einheit des Artikels.
            rabatt: Der Rabatt in Prozent (default: 0).
            von: Das Startdatum für die Beschreibung (default: None).
            bis: Das Enddatum für die Beschreibung (default: None).
            vertrag: Der Vertragstext (default: None).
            beschreibung: Eine zusätzliche optionale Beschreibung (default: None).
        """
        artikel_doc = frappe.get_doc("Item", artikel_code)

        # Erstelle die Beschreibung basierend auf den übergebenen Daten
        von_str = datetime.strftime(von, "%d.%m.%Y") if von else ""
        bis_str = datetime.strftime(bis, "%d.%m.%Y") if bis else ""
        
        # Vertragstext prüfen
        desc = str(vertrag) if vertrag and str(vertrag) != "0" else ""

        # Beschreibung zusammenstellen
        artikel_beschreibung = artikel_doc.item_name + f"<br>Zeitraum von {von_str} bis {bis_str}<br>{desc}"

        # Wenn eine zusätzliche Beschreibung vorhanden ist, hinzufügen
        if beschreibung:
            artikel_beschreibung += f"<br>{beschreibung}"

        artikel_position = frappe.get_doc({
            "doctype": "Sales Invoice Item",
            "item_code": artikel_doc.name,
            "qty": menge,
            "rate": preis,
            "discount_percentage": rabatt,
            "description": artikel_beschreibung
        })

        rechnung_doc.append("items", artikel_position)




    # def create_erpn_invoice(self, rechnungen):
    # return_dict = {"status": False, "fehler": []}
    
    # # Erstellen des Rechnungsdokuments mit der ausgelagerten Funktion
    # rechnung_doc = self.create_rechnung_doc(rechnungen)

    # for position in rechnungen["positionen"]:
    #     artikel_liste = frappe.get_all("Item", filters={"hersteller_artikel_nummer": position["artikel"]})

    #     if len(artikel_liste) == 1:
    #         artikel_doc = frappe.get_doc("Item", artikel_liste[0]["name"])

    #         if not rechnungen["gs_erforderlich"]:
    #             included_service = frappe.get_all("ADN Included Services", filters={"customer": rechnung_doc.customer})
    #             inkludierte_menge = 0

    #             if included_service:
    #                 items = frappe.get_all("ADN Included Services Item", filters={
    #                     "parent": included_service[0].name,
    #                     "item_code": artikel_doc.name
    #                 }, fields=["qty"])

    #                 if items:
    #                     inkludierte_menge = items[0].qty

    #             menge = position["menge"]
    #             preis = position["preis"] if self.preisentnahme_aus == "CSV Datei" else ""

    #             # 1. Position für Inklusivleistung (mit 100% Rabatt)
    #             if inkludierte_menge > 0:
    #                 beschreibung = (artikel_doc.description or "") + "<p><strong>Inklusivleistung</strong></p>"
    #                 self.add_item_to_invoice(rechnung_doc, artikel_doc.name, min(menge, inkludierte_menge), preis, rabatt=100, beschreibung=beschreibung)

    #             # 2. Restliche Position (mit regulärem Preis)
    #             restliche_menge = menge - inkludierte_menge
    #             if restliche_menge > 0:
    #                 self.add_item_to_invoice(rechnung_doc, artikel_doc.name, restliche_menge, preis)

    #             # Normale Position, wenn keine Inklusivleistung vorhanden
    #             if inkludierte_menge == 0:
    #                 self.add_item_to_invoice(rechnung_doc, artikel_doc.name, menge, preis)

    #         else:  # Wenn gs_erforderlich == True
    #             menge = -position["menge"]
    #             preis_csv = -position["preis"]
    #             preis = preis_csv / 0.825
    #             self.add_item_to_invoice(rechnung_doc, artikel_doc.name, menge, preis)

    #     else:
    #         if len(artikel_liste) == 0:
    #             meldung = f"Zu Herstellerartikelnummer {position['artikel']} konnte kein Artikel gefunden werden"
    #             return_dict["fehler"].append(meldung)
    #         else:
    #             meldung = f"Zu Herstellerartikelnummer {position['artikel']} wurden mehrere Artikel gefunden, daher konnte keine eindeutige Zuordnung erfolgen"
    #             return_dict["fehler"].append(meldung)

    # if not return_dict["fehler"]:
    #     rechnung_doc.save()
    #     rechnung_doc.set_taxes()
    #     rechnung_doc.save()
    #     rg_nr = rechnung_doc.name
    #     return_dict["status"] = rg_nr

    # return return_dict


    # def create_erpn_invoice(self, rechnungen):
    #     return_dict = {"status": False, "fehler": []}
    #     rechnung_doc = frappe.get_doc({
    #         "doctype": "Sales Invoice"
    #     })

    #     rechnungsdatum = datetime.strptime(rechnungen["datum"], "%d.%m.%Y %H:%M:%S")
    #     rechnungsmonat = datetime.strftime(rechnungsdatum, "%m.%Y")

    #     if rechnungen["gs_erforderlich"]:
    #         rechnung_doc.is_return = 1
    #         rechnung_doc.title = f"{self.settings_doc.title_and_prefix_credit_note} {rechnungsmonat}"
    #     else:
    #         rechnung_doc.title = f"{self.settings_doc.title_and_prefix} {rechnungsmonat}"

    #     rechnung_doc.customer = rechnungen["kdnr"]
    #     rechnung_doc.introduction_text = self.settings_doc.introduction_text
    #     rechnung_doc.company = self.settings_doc.company
    #     customer_doc = frappe.get_doc("Customer", rechnungen["kdnr"])

    #     rechnung_doc.payment_terms_template = customer_doc.payment_terms or self.settings_doc.payment_terms_template
    #     rechnung_doc.tc_name = self.settings_doc.tc_name

    #     tac_doc = frappe.get_doc("Terms and Conditions", self.settings_doc.tc_name)
    #     rechnung_doc.terms = tac_doc.terms

    #     rechnung_doc.taxes_and_charges = party_st(rechnung_doc.customer, "Customer", rechnung_doc.posting_date, rechnung_doc.company)
    #     taxes = frappe.get_doc("Sales Taxes and Charges Template", rechnung_doc.taxes_and_charges).taxes

    #     for tax in taxes:
    #         new_tax = frappe.get_doc({
    #             "doctype": "Sales Taxes and Charges",
    #             "charge_type": tax.charge_type,
    #             "account_head": tax.account_head,
    #             "rate": tax.rate,
    #             "description": tax.description
    #         })
    #         rechnung_doc.append("taxes", new_tax)

    #     rechnung_doc.adn_invoice_number = rechnungen["adn_rg"]

    #     for position in rechnungen["positionen"]:
    #         artikel_liste = frappe.get_all("Item", filters={"hersteller_artikel_nummer": position["artikel"]})

    #         if len(artikel_liste) == 1:
    #             artikel_doc = frappe.get_doc("Item", artikel_liste[0]["name"])

    #             if not rechnungen["gs_erforderlich"]:
    #                 included_service = frappe.get_all("ADN Included Services", filters={"customer": rechnung_doc.customer})
    #                 inkludierte_menge = 0

    #                 if included_service:
    #                     items = frappe.get_all("ADN Included Services Item", filters={
    #                         "parent": included_service[0].name,
    #                         "item_code": artikel_doc.name
    #                     }, fields=["qty"])

    #                     if items:
    #                         inkludierte_menge = items[0].qty

    #                 menge = position["menge"]
    #                 preis = position["preis"] if self.preisentnahme_aus == "CSV Datei" else ""

    #                 if inkludierte_menge > 0:
    #                     rechnung_doc_artikel_frei = frappe.get_doc({
    #                         "doctype": "Sales Invoice Item",
    #                         "item_code": artikel_doc.name,
    #                         "qty": min(menge, inkludierte_menge),
    #                         "rate": preis,
    #                         "discount_percentage": 100
    #                     })

    #                     neue_beschreibung = (artikel_doc.description or "") + "<p><strong>Inklusivleistung</strong></p>"
    #                     rechnung_doc_artikel_frei.description = neue_beschreibung
    #                     rechnung_doc.append("items", rechnung_doc_artikel_frei)

    #                 restliche_menge = menge - inkludierte_menge
    #                 if restliche_menge > 0:
    #                     rechnung_doc_artikel_berechnet = frappe.get_doc({
    #                         "doctype": "Sales Invoice Item",
    #                         "item_code": artikel_doc.name,
    #                         "qty": restliche_menge,
    #                         "rate": preis
    #                     })
    #                     rechnung_doc.append("items", rechnung_doc_artikel_berechnet)

    #                 if inkludierte_menge == 0:
    #                     rechnung_doc_artikel = frappe.get_doc({
    #                         "doctype": "Sales Invoice Item",
    #                         "item_code": artikel_doc.name,
    #                         "qty": menge,
    #                         "rate": preis
    #                     })
    #                     rechnung_doc.append("items", rechnung_doc_artikel)

    #             else:  # Wenn gs_erforderlich == True
    #                 menge = -position["menge"]
    #                 preis_csv = -position["preis"]
    #                 preis = preis_csv / 0.825
    #                 rechnung_doc_artikel = frappe.get_doc({
    #                     "doctype": "Sales Invoice Item",
    #                     "item_code": artikel_doc.name,
    #                     "qty": menge,
    #                     "rate": preis
    #                 })
    #                 rechnung_doc.append("items", rechnung_doc_artikel)

    #         else:
    #             if len(artikel_liste) == 0:
    #                 meldung = f"Zu Herstellerartikelnummer {position['artikel']} konnte kein Artikel gefunden werden"
    #                 return_dict["fehler"].append(meldung)
    #             else:
    #                 meldung = f"Zu Herstellerartikelnummer {position['artikel']} wurden mehrere Artikel gefunden, daher konnte keine eindeutige Zuordnung erfolgen"
    #                 return_dict["fehler"].append(meldung)

    #     if not return_dict["fehler"]:
    #         rechnung_doc.save()
    #         rechnung_doc.set_taxes()
    #         rechnung_doc.save()
    #         rg_nr = rechnung_doc.name
    #         return_dict["status"] = rg_nr

    #     return return_dict

def test_included_services(customer):
    print(customer)
    # Suche nach Inklusivleistungen für den Kunden und den Artikel in der Childtable "Items"
    included_service = frappe.get_all("ADN Included Services", filters={
        "customer": customer})
    items = frappe.get_all("ADN Included Services Item",filters={"parent":included_service[0].name})
   
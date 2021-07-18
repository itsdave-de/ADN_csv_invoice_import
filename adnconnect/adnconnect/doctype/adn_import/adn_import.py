# -*- coding: utf-8 -*-
# Copyright (c) 2021, itsdave and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from pprint import pprint
from frappe.utils import file_manager
from frappe.model.document import Document
from datetime import datetime
import pandas as pd



class ADNImport(Document):
    log_list = []
    settings_doc = frappe.get_single("ADN Import Settings")
    def do_import(self):
        
        log_list = self.log_list 
        
        count_erfolgreich_erstellte_rechnung = 0
        rechnungen = self.get_invoice_dict_from_csv()
        
        for rechnung in rechnungen:
            log_eintrag_vorhanden = False
            if self.check_adn_invoice_number(rechnung):
               

                customer_exists = self.check_erpn_customer(rechnung["kdnr"])
                
                
                if not customer_exists:
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
           
        log_list.append(str(count_erfolgreich_erstellte_rechnung) + " Rechnungen wurden erstellt")
        log_str = ""
        for i in log_list:
            log_str += i + "\n"
        
        
        self.log = log_str
        self.save()
        

    def get_invoice_dict_from_csv(self):   
        csv_file = frappe.utils.file_manager.get_file_path(self.datei)
        rechnungen = []
        rechnung ={}
        
        df = pd.read_csv(csv_file, encoding='utf-8-sig', delimiter=";", decimal=",") 
        df = df.fillna(0)
        
        

        aktuelle_rg = ""
        erste_zeile = list(df.columns)
        

        if self.validate_csv(erste_zeile):

            for row, pos in df.iterrows():

                                            
                if pos['RECHNUNG'] != aktuelle_rg:
                                        
                       
                    if aktuelle_rg != "":
                        rechnungen.append(rechnung)
                                        
                    aktuelle_rg = pos['RECHNUNG']
   
                                    
                    rechnung ={}
                    #Beginn neuer Rechnung, Kopfdaten auslesen
                    rechnung["kdnr"] =  pos['Endkunde_Reference']
                    rechnung["adn_rg"] = pos['RECHNUNG']
                    rechnung["kunde"] = pos['Endkunde']
                    rechnung["positionen"] = []
                         

                else:
                    #für jede weitere Zeile einer Rechnung erkennen wir
                    #die weiteren Positionen
                                    
                    position = {"artikel": pos['HERSTELLERNUMMER'],
                            "von": pos['Wartungsbeginn'],
                            "bis": pos['Wartungsende'], 
                            "menge": pos['MENGE'], 
                            "preis": pos['Listpreis']}
                                        
                    rechnung["positionen"].append(position)
                        
                
            rechnungen.append(rechnung)
             
            
            return(rechnungen)
        else:
            frappe.msgprint("ACHTUNG Rechnungen konnten nicht erstellt werden. CSV- Format stimmt nicht mit dem Standartformat überein")
                
        
            

    def validate_csv(self, erste_zeile):
         
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
       'MSERP', 'MSERP_BillingPeriod']
        
        if soll_header == erste_zeile:
            self.log_list.append("CSV wurde erfolgreich überprüft")
            return True

        else:
            self.log_list.append("CSV Format entspricht nicht dem Standartformat")
            return False   

    def check_adn_invoice_number(self,rechnung):
        ausgangs_rechnungen =  frappe.get_all("Sales Invoice", filters={"adn_invoice_number": rechnung["adn_rg"] })
        if len(ausgangs_rechnungen) == 0:
            return True

        else:
            self.log_list.append("Zu der ADN-Rechnungs-Nr.: " + str(rechnung["adn_rg"]) + " existiert bereits eine Rechnung, es wurde keine neue Rechnung erstellt")      
            return False
            
    def create_erpn_invoice(self, rechnung):
        return_dict = {"status": False, "fehler": []}
        rechnung_doc = frappe.get_doc({
            "doctype": "Sales Invoice"
            })

        rechnung_doc.customer = rechnung["kdnr"]
        rechnung_doc.introduction_text = self.settings_doc.introduction_text
        rechnung_doc.company = self.settings_doc.company
        rechnung_doc.payment_terms_template = self.settings_doc.payment_terms_template
        rechnung_doc.tc_name = self.settings_doc.tc_name
        rechnung_doc.title = self.settings_doc.title_and_preatext+ " " + str(rechnung["kunde"])
        rechnung_doc.adn_invoice_number = rechnung["adn_rg"]


        for position in rechnung["positionen"]:
            artikel_liste = frappe.get_all("Item", filters={"hersteller_artikel_nummer": position["artikel"]})
        
            if len(artikel_liste) == 1:
                artikel_doc = frappe.get_doc("Item", artikel_liste[0]["name"])
            

                rechnung_doc_artikel = frappe.get_doc({
                    "doctype": "Sales Invoice Item",
                    "item_code": artikel_doc.name,
                    "qty": position["menge"]
                    })
            
            

                von_dt = datetime.strptime (position["von"],"%d.%m.%Y %H:%M:%S")
                von_str = datetime.strftime(von_dt, "%d.%m.%Y")
                bis_dt = datetime.strptime (position["bis"],"%d.%m.%Y %H:%M:%S")
                bis_str = datetime.strftime(bis_dt, "%d.%m.%Y")


                rechnung_doc_artikel.description = artikel_doc.description + "<br>Zeitraum von " + von_str+ " bis "+ bis_str
                rechnung_doc.append("items", rechnung_doc_artikel)

            else:
                if len(artikel_liste) == 0:
                    meldung = "Zu Herstellerartilelnummer " + position["artikel"] + " konnte kein Artikel gefunden werden"
                    return_dict["fehler"].append(meldung)
                if len(artikel_liste) > 1:
                    meldung = "Zu Herstellerartilelnummer " + position["artikel"] + " wurden mehrere Artikel gefunden, daher konnte keine eindeutige Zuordnung erfolgen"
                    return_dict["fehler"].append(meldung)
                
        if len(return_dict["fehler"]) == 0:
            rechnung_doc.save()
            rg_nr = rechnung_doc.name
            return_dict["status"] = rg_nr

        return(return_dict)
    
    

    def check_erpn_customer(self, kundennummer):
        
        
        if kundennummer == 0:
            return False
        else:     
            kunden_liste = frappe.get_all("Customer", filters={"name":kundennummer })
            
            if len(kunden_liste) == 1:
                return True

            

# Copyright (c) 2022, itsdave and contributors
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

class ADNHornetImport(Document):
	log_list = []
	settings_doc = frappe.get_single("ADN Import Settings")

	def get_column_value(self, row, column_name, normalized_columns, default_value=None):
		"""
		Flexibler Spaltenzugriff mit Fallback-Werten
		"""
		# Versuche direkten Zugriff
		if column_name in row:
			return row[column_name]
		
		# Versuche normalisierten Zugriff (Großbuchstaben)
		upper_column = column_name.upper()
		if upper_column in normalized_columns:
			actual_column = normalized_columns[upper_column]
			if actual_column in row:
				return row[actual_column]
		
		# Fallback: Suche nach ähnlichen Spaltennamen
		for col in row.index:
			if col.upper() == upper_column:
				return row[col]
		
		# Wenn nichts gefunden, verwende Standardwert
		self.log_list.append(f"Warnung: Spalte '{column_name}' nicht gefunden, verwende Standardwert: {default_value}")
		return default_value

	def parse_datetime_robust(self, date_string):
		"""
		Robustes Datums-Parsing mit mehreren Formaten und Fehlerkorrektur
		"""
		if not date_string or pd.isna(date_string):
			return None
		
		date_string = str(date_string).strip()
		
		# Verschiedene Datumsformate versuchen
		formats_to_try = [
			"%d.%m.%Y %H:%M",      # 26.02.2025 14:30
			"%d.%m.%Y %H:%M:%S",   # 26.02.2025 14:30:00
			"%d.%m.%Y",            # 26.02.2025
			"%Y-%m-%d %H:%M:%S",   # 2025-02-26 14:30:00
			"%Y-%m-%d %H:%M",      # 2025-02-26 14:30
			"%Y-%m-%d",            # 2025-02-26
		]
		
		for fmt in formats_to_try:
			try:
				return datetime.strptime(date_string, fmt)
			except ValueError:
				continue
		
		# Wenn kein Format funktioniert, versuche pandas to_datetime
		try:
			return pd.to_datetime(date_string).to_pydatetime()
		except:
			pass
		
		# Letzter Versuch: Manuelle Bereinigung und Parsing
		try:
			# Entferne alle nicht-numerischen Zeichen außer Punkten
			cleaned = ''.join(c for c in date_string if c.isdigit() or c == '.')
			if len(cleaned) >= 8:  # Mindestens DD.MM.YYYY
				parts = cleaned.split('.')
				if len(parts) >= 3:
					day = int(parts[0])
					month = int(parts[1])
					year = int(parts[2])
					if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
						return datetime(year, month, day)
		except:
			pass
		
		# Wenn alles fehlschlägt, logge Fehler und gib None zurück
		self.log_list.append(f"Fehler: Konnte Datum '{date_string}' nicht parsen")
		return None

	@frappe.whitelist()
	def do_import(self):
        
		log_list = self.log_list 
        #gezählt werden vollstädig erstellte und vorbereitete Rechnungen
		count_erfolgreich_erstellte_rechnung = 0
		rechnungen = self.get_invoice_dict_from_csv()
		print(rechnungen)
		lizenzen = 0
		for rechnung in rechnungen:
			print(rechnung)
            #gezählt wird die Gesamtzahl der Lizenzen
			for position in rechnung["positionen"]:
				if float(position["preis"]) > 0: 
					lizenzen += int(position["menge"])
          
			log_eintrag_vorhanden = False
			if self.check_adn_invoice_number(rechnung):
            
				customer_exists = self.check_erpn_customer(rechnung["kdnr"])
				print(customer_exists)
                                    
				if not customer_exists:
                    #wenn der Kunde nicht in ERPNext existiert, verwende default_customer
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
        
		rechnungsdatum = self.parse_datetime_robust(rechnung["datum"])
		if rechnungsdatum:
			self.rechnungsdatum = datetime.strftime(rechnungsdatum, "%m.%Y")
		else:
			self.rechnungsdatum = "08.2025"  # Fallback
			self.log_list.append(f"Warnung: Konnte Rechnungsdatum '{rechnung['datum']}' nicht parsen, verwende Fallback")
		self.anzahl_der_lizenzen = lizenzen
			
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
				print(gs_dict,rg_dict)

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
		print(df)

		kunde= ""
		erste_zeile = list(df.columns)
		#print(erste_zeile)

		if self.validate_csv(erste_zeile):
			# Spaltennormalisierung nach erfolgreicher Validierung
			normalized_columns = {col.upper(): col for col in df.columns}
			self.log_list.append("Spaltennormalisierung durchgeführt")

			for row, pos in df.iterrows():
                               
				endkunde = self.get_column_value(pos, 'Endkunde', normalized_columns, "")
				if endkunde != kunde:
											
					if kunde != "":
						rechnungen.append(rechnung)
										
					kunde = endkunde

					rechnung = {}
					# Neue Spaltenlogik: Versuche zuerst Endkunde_Reference, falls nicht vorhanden, verwende Endkunde
					endkunde_reference = self.get_column_value(pos, 'Endkunde_Reference', normalized_columns, "")
					if not endkunde_reference:
						endkunde_reference = self.get_column_value(pos, 'Endkunde', normalized_columns, "")
						self.log_list.append(f"Info: Verwende 'Endkunde' als Referenz, da 'Endkunde_Reference' nicht gefunden wurde")
					
					customer = frappe.get_all("Customer", filters = {"hornet_domain": endkunde_reference}, fields = ["name","customer_name"])
					print(customer)
					
					# Prüfe ob Kunde gefunden wurde
					if len(customer) == 1:
						cust = customer[0]["name"]
						print(customer[0]["customer_name"], customer[0]["name"])
						#Beginn neuer Rechnung, Kopfdaten auslesen
						rechnung["kdnr"] = cust
						rechnungsnummer = self.get_column_value(pos, 'RECHNUNG', normalized_columns, "")
						rechnung["adn_rg"] = str(rechnungsnummer)+ cust
						rechnung["kunde"] = customer[0].customer_name
					elif len(customer) > 1:
						# Mehrere Kunden gefunden - logge Fehler und verwende Default-Kunde
						rechnungsnummer = self.get_column_value(pos, 'RECHNUNG', normalized_columns, "")
						self.log_list.append(f"Warnung: Mehrere Kunden mit Referenznummer '{endkunde_reference}' gefunden (Rechnung: {rechnungsnummer}), verwende Default-Kunde")
						rechnung["kdnr"] = self.settings_doc.default_customer
						# Eindeutige Nummer für Dummy-Rechnung mit Zeitstempel
						import time
						timestamp = int(time.time() * 1000) % 10000  # 4-stellige eindeutige Nummer
						rechnung["adn_rg"] = str(rechnungsnummer) + "DUMMY" + str(timestamp)
						rechnung["kunde"] = f"Mehrfache Zuordnung: {endkunde_reference}"
						rechnung["needs_review"] = True
					else:
						# Kein Kunde gefunden - verwende Default-Kunde
						rechnungsnummer = self.get_column_value(pos, 'RECHNUNG', normalized_columns, "")
						self.log_list.append(f"Warnung: Referenznummer '{endkunde_reference}' konnte keinem Kunden zugeordnet werden (Rechnung: {rechnungsnummer}), verwende Default-Kunde")
						rechnung["kdnr"] = self.settings_doc.default_customer
						# Eindeutige Nummer für Dummy-Rechnung mit Zeitstempel
						import time
						timestamp = int(time.time() * 1000) % 10000  # 4-stellige eindeutige Nummer
						rechnung["adn_rg"] = str(rechnungsnummer) + "DUMMY" + str(timestamp)
						rechnung["kunde"] = f"Unbekannter Kunde: {endkunde_reference}"
						rechnung["needs_review"] = True
					rechnungsart = self.get_column_value(pos, 'Rechnungsart', normalized_columns, "")
					rechnung["art"] = rechnungsart
					rechnung["datum"] = self.get_column_value(pos, 'DATUM', normalized_columns, "")
					rechnung["positionen"] = []
					rechnung["gs_erforderlich"] = False 
					
					wartungsbeginn = self.get_column_value(pos, 'Wartungsbeginn', normalized_columns, "")
					wartungsende = self.get_column_value(pos, 'Wartungsende', normalized_columns, "")
					von_dt = self.parse_datetime_robust(str(wartungsbeginn))
					bis_dt = self.parse_datetime_robust(str(wartungsende))
					
					# Prüfe ob Datums-Parsing erfolgreich war
					if von_dt is None or bis_dt is None:
						self.log_list.append(f"Fehler: Konnte Wartungszeitraum nicht parsen für Rechnung {rechnungsnummer}")
						continue
					
					print(von_dt, bis_dt)
					time_delta = bis_dt - von_dt 
                                    
					hersteller_nummer = self.get_column_value(pos, 'HERSTELLERNUMMER', normalized_columns, "")
					menge = self.get_column_value(pos, 'MENGE', normalized_columns, 0)
					listpreis = self.get_column_value(pos, 'Listpreis', normalized_columns, 0)
					vertrag = self.get_column_value(pos, 'Vertrag', normalized_columns, "")
					
					position = {"artikel": hersteller_nummer,
							"von": von_dt,
							"bis": bis_dt,  
							"menge": menge, 
							"preis": listpreis,
							"wartungsdauer": time_delta.days+1,
							"gesamtdauer": calendar.monthrange(von_dt.year,von_dt.month)[1],
							"vertrag" : vertrag
							}
					if float(listpreis)<0:
						rechnung["gs_erforderlich"] = True
											
					rechnung["positionen"].append(position)
					
				else:
					#für jede weitere Zeile einer Rechnung erkennen wir
					#die weiteren Positionen
					wartungsbeginn = self.get_column_value(pos, 'Wartungsbeginn', normalized_columns, "")
					wartungsende = self.get_column_value(pos, 'Wartungsende', normalized_columns, "")
					von_dt = self.parse_datetime_robust(str(wartungsbeginn))
					bis_dt = self.parse_datetime_robust(str(wartungsende))
					
					# Prüfe ob Datums-Parsing erfolgreich war
					if von_dt is None or bis_dt is None:
						self.log_list.append(f"Fehler: Konnte Wartungszeitraum nicht parsen für Position in Rechnung {kunde}")
						continue
					
					print(von_dt, bis_dt)
					time_delta = bis_dt - von_dt 
								
					hersteller_nummer = self.get_column_value(pos, 'HERSTELLERNUMMER', normalized_columns, "")
					menge = self.get_column_value(pos, 'MENGE', normalized_columns, 0)
					listpreis = self.get_column_value(pos, 'Listpreis', normalized_columns, 0)
					vertrag = self.get_column_value(pos, 'Vertrag', normalized_columns, "")
					
					position = {"artikel": hersteller_nummer,
						"von": von_dt,
						"bis": bis_dt,  
						"menge": menge, 
						"preis": listpreis,
						"wartungsdauer": time_delta.days+1,
						"gesamtdauer": calendar.monthrange(von_dt.year,von_dt.month)[1],
						"vertrag" : vertrag
						}
					if float(listpreis)<0:
						rechnung["gs_erforderlich"] = True
											
					rechnung["positionen"].append(position)
			#print(rechnung)	
			rechnungen.append(rechnung)
			#rechnungen = self.erzeuge_gs_wenn_erforderlich(rechnungen) 
			#print("Rechnungen: ")
			#print(rechnungen)       
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
		'MSERP', 'MSERP_BillingPeriod']
        
        
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
		ausgangs_rechnungen =  frappe.get_all("Sales Invoice", filters={"adn_invoice_number": str(rechnung["adn_rg"])+str(rechnung["kdnr"]) })
		if len(ausgangs_rechnungen) == 0:
			return True
		else:
			self.log_list.append("Für den Kunden: " + rechnung["kunde"] + " existiert bereits eine Rechnung, es wurde keine neue Rechnung erstellt")      
			return False

	def create_erpn_invoice(self, rechnungen):
		return_dict = {"status": False, "fehler": []}
		rechnung_doc = frappe.get_doc({
				"doctype": "Sales Invoice"
				})
		
			
		rechnungsdatum = self.parse_datetime_robust(rechnungen["datum"])
		if rechnungsdatum:
			rechnungsmonat = datetime.strftime(rechnungsdatum, "%m.%Y")
		else:
			rechnungsmonat = "01.2025"  # Fallback
			self.log_list.append("Warnung: Konnte Rechnungsdatum nicht parsen, verwende Fallback")
		print(rechnungsmonat)
		
		if rechnungen["gs_erforderlich"] == True:
			rechnung_doc.is_return = 1
			rechnung_doc.title = str(self.settings_doc.title_and_prefix_credit_note) + " " + rechnungsmonat
		else:
			rechnung_doc.title = str(self.settings_doc.title_and_prefix_hornet)+ " " + rechnungsmonat

		rechnung_doc.customer = rechnungen["kdnr"]
		rechnung_doc.introduction_text = self.settings_doc.introduction_text_hornet
		rechnung_doc.company = self.settings_doc.company
		customer_doc = frappe.get_doc("Customer", rechnungen["kdnr"] )
        #print(customer_doc.payment_terms)
		if customer_doc.payment_terms:
			rechnung_doc.payment_terms_template = customer_doc.payment_terms
		else:
			rechnung_doc.payment_terms_template = self.settings_doc.payment_terms_template
		rechnung_doc.tc_name = self.settings_doc.tc_name

		tac_doc = frappe.get_doc("Terms and Conditions", self.settings_doc.tc_name)
		rechnung_doc.terms = tac_doc.terms
		
		rechnung_doc.taxes_and_charges = party_st(rechnung_doc.customer, "Customer", rechnung_doc.posting_date, rechnung_doc.company)
		taxes = frappe.get_doc("Sales Taxes and Charges Template", rechnung_doc.taxes_and_charges).taxes
		#im Doctype hinterlegte Steuern müssen der Rechnung angefügt werden.
		#Die Berechnung der Beträge geschieht automatisch
		for tax in taxes:
			new_tax = frappe.get_doc({
				"doctype": "Sales Taxes and Charges",
				"charge_type": tax.charge_type,
				"account_head": tax.account_head,
				"rate": tax.rate,
				"description": tax.description
			})
			rechnung_doc.append("taxes", new_tax)
		
		rechnung_doc.adn_invoice_number = rechnungen["adn_rg"]+rechnungen["kdnr"]

		for position in rechnungen["positionen"]:
			artikel_liste = frappe.get_all("Item", filters={"hersteller_artikel_nummer": position["artikel"]})
		
			if len(artikel_liste) == 1:
				
				artikel_doc = frappe.get_doc("Item", artikel_liste[0]["name"])
				if rechnungen["gs_erforderlich"] == False:
					
					menge = position["menge"]*position["wartungsdauer"]/position["gesamtdauer"]
					preis = ""
				else:
					menge = -position["menge"]
					preis = -position["preis"]

				rechnung_doc_artikel = frappe.get_doc({
					"doctype": "Sales Invoice Item",
					"item_code": artikel_doc.name,
					"qty": menge,
					"rate": preis

					})
			
				von_str = datetime.strftime(position["von"], "%d.%m.%Y")
				bis_str = datetime.strftime(position["bis"], "%d.%m.%Y")
				
				rechnung_doc_artikel.description = artikel_doc.item_name + "<br>Zeitraum von " + von_str+ " bis "+ bis_str
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
			rechnung_doc.set_taxes()
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
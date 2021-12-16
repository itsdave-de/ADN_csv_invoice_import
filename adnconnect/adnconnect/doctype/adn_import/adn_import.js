// Copyright (c) 2021, itsdave and contributors
// For license information, please see license.txt

frappe.ui.form.on('ADN Import', {
	// refresh: function(frm) {

	// }
});
frappe.listview_settings['ADN Import'] = {
    
	get_indicator:function(doc){
	if (doc.status === "neu") {
				return [__("neu"), "blue", "status,=,neu"];
	}
	if (doc.status=="fehlerhaft" ) {
		return [__("fehlerhaft"), "red", "status,=,fehlerhaft"];
	}
	if (doc.status === "erfolgreich") {
		return [__("erfolgreich"), "green", "status,=,erfolgreich"];
	}
	
}}
 
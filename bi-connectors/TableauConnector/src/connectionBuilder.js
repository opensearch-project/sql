(function dsbuilder(attr){
	var connStr = "jdbc:opensearch://";
	// Set SSL value in connection string 
	if (attr[connectionHelper.attributeSSLMode] == "require"){
		connStr += "https://";
	} else {
		connStr += "http://";
	}

	// Set host information in connection string
	connStr += attr[connectionHelper.attributeServer] + ":" + attr[connectionHelper.attributePort] + "?";

	// Set authentication values in connection string
	var authAttrValue = attr[connectionHelper.attributeAuthentication];
	if (authAttrValue == "auth-none"){
		connStr += "auth=NONE&trustSelfSigned=" + attr["v-trustSelfSigned"];
	} else if (authAttrValue == "auth-integrated"){
		connStr += "auth=AWS_SIGV4";
		var region = attr["v-region"];
		if (region){
			connStr += "&Region=" + region;
		}
	} else { //if (authAttrValue == "auth-user-pass"){
		connStr += "auth=BASIC&user=" + attr[connectionHelper.attributeUsername] + "&password=" + attr[connectionHelper.attributePassword] + "&trustSelfSigned=" + attr["v-trustSelfSigned"];
	}

	return [connStr];
})

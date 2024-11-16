import ESPNSoccer
dir = "e:/soccer/tmp/"
id = 911414
event,code = ESPNSoccer.import_event(id,dir)
print(code)
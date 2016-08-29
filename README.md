ADS-B SBS-1 software
====================

Java software that reads ADS-B information in SBS-1 format from the wire (usually, port 30003). The software performs:

* Socket I/O
* Decode the CSV format
* Analyze and split into its own messages
* Decide which messages are superfluous and filter them out
* Stage and buffer messages to be sent and persist them for crash safety
* Bundle after a timeout some messages together and send to a designed web service


# Data Flow outline from RPMS to QC tracker

1. RPMS exports to Data Aggregation server everyday at 21:30 AEDT : 05:30 EST
2. Tashrif’s script then process them 22:00 AEDT : 06:00 EST
3. Lochness (every ~1 Hour) 23:00 AEDT : 07:00 EST
    - places then within PHOENIX
    - Lochness sync pushed them to NDA bucket
4. DPACC servers pull data to predict1 (Runs every hour) 00:00 AEDT : 08:00 EST
5. This pipeline will then generate combined CSVs. (lasts 1 Hr, Starts at 01:00 AEDT : 09:00 EST)
6. Owen uses the generated CSVs to generate QC tracker

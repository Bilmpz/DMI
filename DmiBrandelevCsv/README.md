# DMI Brandelev CSV

Henter DMI metObs-data for station 06154 (Brandelev) for:
- humidity
- temp_dry

Periode:
- fra 2026-01-01
- til tidspunktet hvor programmet køres

## Kør

```bash
cd DmiBrandelevCsv

dotnet restore
dotnet run
```

CSV-filen bliver skrevet til:

```text
bin/Debug/net8.0/output/
```

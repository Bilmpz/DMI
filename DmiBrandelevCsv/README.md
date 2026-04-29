# DmiBrandelevCsv

Hjælpeprogram til DMI metObs-data. Programmet har to modes.

## Mode 1 — stationsliste (default)

```bash
cd DmiBrandelevCsv
dotnet run
```

Henter alle aktive stationer fra DMI og filtrerer til dem der måler både
`temp_dry` og `humidity`. Skriver resultatet til:

```
docs/data/stations.json
```

Filen bruges af websiden i `docs/index.html` til postnummersøgning og
nærmeste-station-opslag. Kør denne mode før hvert push for at holde
stationslisten opdateret.

Eksempel-output:

```json
[
  { "id": "06154", "name": "Brandelev", "lat": 55.31, "lon": 11.78 }
]
```

## Mode 2 — observation-CSV for én station

```bash
cd DmiBrandelevCsv
dotnet run -- --station 06154
```

Henter alle `temp_dry`- og `humidity`-observationer for den givne station fra
`2022-10-01` til nu og skriver dem som CSV til:

```
DmiBrandelevCsv/bin/Debug/net10.0/output/station_<id>_<fra>_to_<til>.csv
```

CSV'en filtreres til hele timer (minut 0, sekund 0) og bruger dansk talformat
(`,` som decimalseparator). Den er nyttig til ad-hoc-eksport, men bruges
ikke af websiden — frontenden henter selv data live fra DMI.

## Krav

- .NET 10 SDK
- Internetforbindelse (DMI's metObs-endpoint kræver ingen API-nøgle)

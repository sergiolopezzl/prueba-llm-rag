
import fastavro
from datetime import datetime

### Definición del esquema AVRO
schema = {
    "doc": "Alertas de Terminales",
    "name": "AlertaTerminal",
    "namespace": "com.hubdatos.analytics",
    "type": "record",
    "fields": [
        {"name": "codigo_terminal", "type": "string"},
        {"name": "tipo_alerta", "type": "string"}, 
        {"name": "fecha_evento", "type": "string"},
        {"name": "detalle", "type": "string"}
    ]
}

# Datos de ejemplo (Simulación de salida SQL)
datos = [
    {
        "codigo_terminal": "TERM-001",
        "tipo_alerta": "PAPEL_BAJO",
        "fecha_evento": datetime.now().isoformat(),
        "detalle": "Consumo > 50%"
    },
    {
        "codigo_terminal": "TERM-999",
        "tipo_alerta": "FRAUDE_POSIBLE",
        "fecha_evento": datetime.now().isoformat(),
        "detalle": "Tx en comercio Inactivo"
    }
]

with open('alertas.avro', 'wb') as out:
    fastavro.writer(out, schema, datos)

# Cart Streaming Pipeline

Pipeline de streaming de datos que consume carritos de compra desde DummyJSON,
los publica en Redpanda, los transforma (simulando Apache Flink), y los sirve
vía HTTP a un dashboard web.

## Arquitectura

```
DummyJSON API
      ↓
Producer (Python)
      ↓
Redpanda: raw-carts
      ↓
Flink Simulator (Python)
      ↓
Redpanda: processed-carts
      ↓
Backend (FastAPI)
      ↓
Web Dashboard (HTML + JavaScript)
```

Además:
- `errors-carts`: eventos inválidos o rechazados.

## Requisitos

- Python 3.14+
- Podman
- Redpanda (contenedor)
- Librerías Python:
  - `kafka-python`
  - `requests`
  - `fastapi`
  - `uvicorn`

## Cómo ejecutar

### 1. Arrancar Redpanda

```bash
podman start redpanda
podman exec -it redpanda rpk cluster info
```

### 2. Activar el entorno virtual

```bash
cd ~/Documents/SAMANTHA/Proyectos/PYTHONLESSONS/Exercise.py
source venv/bin/activate
```

### 3. Arrancar el backend

```bash
uvicorn backend:app --reload --port 8000
```

### 4. Publicar carritos (en otra terminal)

```bash
python raw_producer.py
```

### 5. Transformar carritos (en otra terminal)

```bash
python flink_simulator.py
```

### 6. Abrir el dashboard

```bash
xdg-open dashboard.html
```

O acceder a los endpoints en el navegador:

- `http://localhost:8000/docs` → Documentación automática de FastAPI
- `http://localhost:8000/health` → Estado del backend
- `http://localhost:8000/api/carts` → Carritos procesados
- `http://localhost:8000/api/metrics` → Métricas agregadas
- `http://localhost:8000/api/status` → Estado del pipeline

## Endpoints

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Verifica que el backend está vivo |
| `/api/status` | GET | Estado general del pipeline |
| `/api/carts` | GET | Lista de carritos procesados |
| `/api/metrics` | GET | Métricas agregadas |

## Topics

| Topic | Productor | Consumidor | Propósito |
|-------|-----------|------------|-----------|
| `raw-carts` | `raw_producer.py` | `flink_simulator.py` | Carritos crudos desde DummyJSON |
| `processed-carts` | `flink_simulator.py` | Backend | Carritos transformados |
| `errors-carts` | `flink_simulator.py` | Backend | Eventos inválidos o rechazados |

## Transformaciones aplicadas

Cada carrito en `processed-carts` incluye tres campos calculados:

- `discountAmount` = `total` - `discountedTotal`
- `discountPercentage` = (`discountAmount` / `total`) * 100
- `averageProductValue` = `total` / `totalProducts`

Además, se agrega el campo `status: "PROCESSED"`.

## Manejo de errores

Los carritos que fallan la validación se envían a `errors-carts` con esta estructura:

```json
{
  "original": { ... carrito original ... },
  "error": "descripción del error",
  "type": "ValueError"
}
```

Validaciones aplicadas:

- Debe existir la clave `id`
- Deben existir `total`, `discountedTotal`, `totalProducts`
- `totalProducts` no puede ser 0 (evita división por cero)

## Decisiones técnicas

- **Redpanda en lugar de Kafka:** más ligero, sin Zookeeper, compatible con API Kafka.
- **kafka-python:** librería simple para productores y consumidores.
- **FastAPI:** framework moderno con documentación automática.
- **Threading en el backend:** permite consumir Kafka sin bloquear las respuestas HTTP.
- **Flink simulado en Python:** por incompatibilidad de PyFlink con Java 27 y Python 3.14.

## Estructura del proyecto

```
Exercise.py/
├── raw_producer.py         # Productor de raw-carts
├── flink_simulator.py      # Transformador (simula Flink)
├── backend.py              # API HTTP (FastAPI)
├── dashboard.html          # Frontend (HTML + JS)
├── README.md               # Este archivo
└── venv/                   # Entorno virtual de Python
```
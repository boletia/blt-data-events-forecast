# Event Forecast - Predicción de Ventas de Eventos

Sistema de forecasting de ventas de boletos para eventos en vivo usando machine learning. La aplicación combina datos de venues, demografía, popularidad de artistas y estrategia de precios para predecir el porcentaje de sold out y revenue esperado.

## 🎯 Características

- **Doble modelo predictivo**: Compara predicciones entre modelo original y modelo entrenado con datos sintéticos
- **Integración con Chartmetric**: Obtiene métricas en tiempo real de Spotify, Instagram, YouTube y TikTok
- **Análisis demográfico**: Considera población y niveles de ingreso por estado
- **Datos de venues**: Utiliza información de capacidad, ubicación y ratings
- **Interfaz intuitiva**: Aplicación web construida con Streamlit

## 📋 Requisitos Previos

- Python 3.8+
- Cuenta de Snowflake con acceso a las tablas:
  - `core.places` (datos de venues)
  - `demographics.income_by_city` (datos de INEGI)
- API Key de Chartmetric (refresh token)

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone <repository-url>
cd blt-data-events-forecast
```

### 2. Crear entorno virtual

```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar variables de entorno

Crea un archivo `.env` o exporta las siguientes variables:

```bash
# Conexión a Snowflake
export SNOWFLAKE_USER="tu_usuario"
export SNOWFLAKE_PASSWORD="tu_contraseña"
export SNOWFLAKE_ACCOUNT="tu_cuenta"
export SNOWFLAKE_WAREHOUSE="tu_warehouse"
export SNOWFLAKE_DATABASE="tu_database"
export SNOWFLAKE_SCHEMA="tu_schema"

# API de Chartmetric
export CM_APIKEY="tu_refresh_token_de_chartmetric"

# Opcional: para deployment local
export DEPLOY_LOCAL="true"
```

### 5. Ejecutar la aplicación

```bash
streamlit run event_forecast_form.py
```

La aplicación se abrirá automáticamente en `http://localhost:8501`

## 🎮 Uso

1. **Seleccionar Venue**: Elige el lugar del evento desde el dropdown
2. **Ingresar Precios**:
   - Precio mínimo del boleto (MXN)
   - Precio máximo del boleto (MXN)
   - Total de boletos a la venta
   - Face value total (para calcular precio promedio)
3. **Buscar Artista**: Escribe el nombre del artista
4. **Seleccionar Artista**: Elige entre los 5 resultados mostrados (con foto, métricas de Spotify, etc.)
5. **Obtener Predicciones**: Haz clic en el botón para generar los forecasts

## 📊 Modelos

### Modelo Original
- **Features**: 19 variables
- **Incluye**: Rating del venue, población estatal, listeners de Spotify por estado, métricas de TikTok
- **Archivo**: `model.pkl` + `preprocessor.pkl`

### Modelo con Datos Sintéticos
- **Features**: 13 variables (subset optimizado)
- **Incluye**: Métricas clave de redes sociales, ratios de affordability
- **Archivo**: `model2.pkl` + `preprocessor2.pkl`

Ambos modelos predicen:
- **Sold out %**: Porcentaje de boletos vendidos
- **Tickets vendidos**: Cantidad estimada de boletos
- **Face value total**: Revenue esperado (MXN)

## 🏗️ Arquitectura

```
┌─────────────────┐
│   Streamlit UI  │
└────────┬────────┘
         │
    ┌────┴────────────────────────┐
    │                             │
┌───┴──────┐              ┌───────┴──────┐
│ Model 1  │              │   Model 2    │
│(Original)│              │ (Synthetic)  │
└───┬──────┘              └───────┬──────┘
    │                             │
    └─────────┬───────────────────┘
              │
     ┌────────┴─────────┐
     │                  │
┌────┴─────┐    ┌───────┴────────┐
│Snowflake │    │ Chartmetric API│
│  (Venues │    │  (Artist Data) │
│  + Demo) │    │                │
└──────────┘    └────────────────┘
```

## 📁 Estructura del Proyecto

```
.
├── event_forecast_form.py    # Aplicación principal de Streamlit
├── utils.py                   # Funciones de utilidad y data fetching
├── requirements.txt           # Dependencias Python
├── model.pkl                  # Modelo XGBoost original
├── preprocessor.pkl           # Preprocesador modelo original
├── model2.pkl                 # Modelo XGBoost datos sintéticos
├── preprocessor2.pkl          # Preprocesador modelo sintético
├── model2_old.pkl             # Versión anterior (backup)
├── preprocessor2_old.pkl      # Versión anterior (backup)
└── README.md                  # Este archivo
```

## 🔑 Features Principales

### Datos de Venue
- Nombre, ciudad, estado
- Rating y total de reviews
- Capacidad
- Coordenadas geográficas

### Datos Demográficos (INEGI)
- Población por estado
- Percentiles de ingreso (30, 50, 70)
- Ratios de affordability calculados

### Métricas de Artista (Chartmetric)
- **Spotify**: Monthly listeners (MX y global), followers, popularity, follower-to-listener ratio
- **Instagram**: Followers
- **YouTube**: Subscribers, views
- **TikTok**: Followers, likes

### Datos de Pricing
- Precio mínimo, promedio y máximo
- Total de boletos disponibles
- Face value total

## 🛠️ Tecnologías

- **Streamlit** 1.29.0 - Framework web
- **XGBoost** 1.7.3 - Modelos de machine learning
- **Pandas** 1.5.2 - Manipulación de datos
- **Snowflake Connector** 2.8.3 - Conexión a base de datos
- **Requests** 2.28.1 - Llamadas a API
- **Millify** 0.1.1 - Formateo de números

## ⚙️ Consideraciones Técnicas

### Caching
- Los datos de venues se cachean en disco (persist="disk")
- Las llamadas a Chartmetric se cachean por ID de artista
- El cache persiste entre sesiones para optimizar performance

### Rate Limiting
- Chartmetric API: Manejo automático de rate limits (429)
- Retry automático para errores 502
- Throttling de 1.3s entre requests

### Manejo de Errores
- Funciones retornan `None` si las APIs fallan
- Validación mínima de datos nulos
- La app requiere todos los archivos `.pkl` para iniciar

## 🐛 Troubleshooting

| Problema | Solución |
|----------|----------|
| App falla al iniciar | Verifica credenciales de Snowflake |
| Búsqueda de artista no funciona | Verifica que `CM_APIKEY` sea válido |
| Predicciones incorrectas | Asegúrate que los archivos `.pkl` coincidan en versión |
| Performance lenta | Revisa rate limits de Chartmetric API |

## 📝 Notas de Desarrollo

### Para agregar nuevas features:

1. Agregar extracción de datos en `utils.py`
2. Actualizar `get_dataframe()` o `get_dataframe2()`
3. Reentrenar el modelo con la nueva feature
4. Actualizar el preprocessor correspondiente
5. Reemplazar archivos `.pkl`

### Para modificar la UI:

- Los layouts usan `st.columns()` para vistas lado a lado
- `st.session_state` mantiene el ID del artista seleccionado
- CSS custom se puede agregar con `st.markdown(..., unsafe_allow_html=True)`

## 📄 Licencia

[Especificar licencia aquí]

## 👥 Contribuidores

- Desarrollo activo por vnolascoBoletia

## 📞 Soporte

Para preguntas o issues, contactar al equipo de data de Boletia.

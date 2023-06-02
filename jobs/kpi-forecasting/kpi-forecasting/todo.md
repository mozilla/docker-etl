# Tests

- The main goal of the tests should be to track that the refactor doesn't break anything. It should be sufficient to just check the outputs from the mobile and desktop forecasts to make sure they're consistent.
- Add test yaml files

# Refactor

- Add a FitForecast class. Use inheritance to build a Prophet fit/forecast class.
- Delete the Statsforecast models for now. They can be re-implemented using the FitForecast class.
- Make a simple class to write to the DB. There's no reason for all of that logic to live in the writer.
  - The writer should accept a FitForecast object. The FitForecast object will have methods to ensure its forecast output is consistent.
- Rename ForecastDatasets to be more indicative of the fact that that code is querying datasets.
- Simplify yaml files when possible
  - Get rid of the `stop_date`; always forecast 12 months ahead unless overridden
- Accept CLI arguments that can override config args
  - This will enable forecasting over arbitrary time ranges using arbitrary historical periods
- Use `mozanalysis` to query DAU using the Metric Hub definition.
- db write should be optional

from kpi_forecasting.results_processing import ModelPerformanceAnalysis

config_list_search = [
    "search_forecasting_ad_clicks.yaml",
    "search_forecasting_daily_active_users.yaml",
    "search_forecasting_search_count.yaml",
]
search_output_name = "search_validation"

config_list_kpi = ["dau_desktop.yaml", "dau_mobile.yaml"]
kpi_output_name = "kpi_validation"


def main() -> None:
    search_validator = ModelPerformanceAnalysis(
        config_list_search, "moz-fx-data-bq-data-science", "jsnyder", search_output_name
    )
    search_validator.write()
    kpi_validator = ModelPerformanceAnalysis(
        config_list_kpi, "moz-fx-data-bq-data-science", "jsnyder", kpi_output_name
    )
    kpi_validator.write()


if __name__ == "__main__":
    main()

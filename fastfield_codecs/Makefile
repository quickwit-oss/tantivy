DATASETS ?= hdfs_logs_timestamps http_logs_timestamps amazon_reviews_product_ids nooc_temperatures
download:
	@echo "--- Downloading datasets ---"
	mkdir -p datasets
	@for dataset in $(DATASETS); do curl -o - https://quickwit-datasets-public.s3.amazonaws.com/benchmarks/fastfields/$$dataset.txt.gz | gunzip > datasets/$$dataset.txt; done


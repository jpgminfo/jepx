CREATE TABLE "spot_summary" (
	"delivery_date"	NUMERIC NOT NULL,
	"interval"	INTEGER NOT NULL,
	"sell_bid_amount"	NUMERIC,
	"buy_bid_amount"	NUMERIC,
	"total_contract_amount"	NUMERIC,
	"system_price"	NUMERIC,
	"area_price_01"	NUMERIC,
	"area_price_02"	NUMERIC,
	"area_price_03"	NUMERIC,
	"area_price_04"	NUMERIC,
	"area_price_05"	NUMERIC,
	"area_price_06"	NUMERIC,
	"area_price_07"	NUMERIC,
	"area_price_08"	NUMERIC,
	"area_price_09"	NUMERIC,
	"sell_block_total_bid_amount"	NUMERIC,
	"sell_block_total_contract_amount"	NUMERIC,
	"buy_block_total_bid_amount"	NUMERIC,
	"buy_block_total_contract_amount"	NUMERIC,
	PRIMARY KEY("delivery_date","interval")
)
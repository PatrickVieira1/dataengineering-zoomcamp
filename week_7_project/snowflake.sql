CREATE OR REPLACE STORAGE INTEGRATION gas_price
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://final_project_zoomcamp');
  
  
DESC STORAGE INTEGRATION gas_price;

CREATE role my_role;

grant create stage on schema public to role my_role;

grant usage on integration gas_price to role my_role;

use schema gas_prices_brazil.public;

create or replace file format my_parquet_format
type = 'PARQUET'
compression = AUTO;

create stage my_gcs_stage
url = 'gcs://final_project_zoomcamp'
storage_integration = gas_price
file_format = my_parquet_format;

list @my_gcs_stage;


CREATE OR REPLACE TABLE "gas_prices_brazil" (
"regiao_sigla" VARCHAR(10),
  "estado_sigla" VARCHAR(10),
  "municipio" VARCHAR(50),
  "revenda" VARCHAR(250),
  "cnpj_revenda" VARCHAR(50),
  "endereço" VARCHAR(250),
  "numero_rua" VARCHAR(250),
  "complemento" VARCHAR(250),
  "bairro" VARCHAR(250),
  "cep" VARCHAR(50),
  "produto" VARCHAR(50),
  "data_da_coleta" VARCHAR(250),
  "valor_venda" FLOAT,
  "valor_compra" FLOAT,
  "unidade_medida" VARCHAR(250),
  "bandeira" VARCHAR(250)
);

copy into public."gas_prices_brazil" 
from @my_gcs_stage
file_format = my_parquet_format
match_by_column_name = case_insensitive
on_error=continue;

select * from public."gas_prices_brazil"
limit 30;

select count(*) from public."gas_prices_brazil"
where "data_da_coleta" is null;

grant all on warehouse compute_wh to role accountadmin;

select * from GAS_PRICES_BRAZIL.PUBLIC."gas_prices_brazil"
limit 30;

select distinct("unidade_medida") from public."gas_prices_brazil"
group by "unidade_medida";

select
    *
from
    public.raw_data
order by
    data_coleta desc
limit 30;

select "bandeira", count(*) from fact_gas_prices
group by "bandeira";
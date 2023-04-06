{{ config(materialized='view') }}


select 
    "regiao_sigla",
    "estado_sigla",
    "municipio",
    "revenda",
    "produto",
    date("data_da_coleta") as data_coleta,
    round("valor_venda", 2) as valor_venda,
    round("valor_compra", 2) as valor_compra,
    "unidade_medida",
    "bandeira"
from 
    {{ source('staging', 'gas_prices_brazil')}}
where
    data_coleta is not null
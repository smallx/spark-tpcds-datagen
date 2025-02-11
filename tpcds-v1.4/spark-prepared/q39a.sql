WITH inv AS
(SELECT
    w_warehouse_name,
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    stdev,
    mean,
    CASE mean
    WHEN 0
      THEN NULL
    ELSE stdev / mean END cov
  FROM (SELECT
    w_warehouse_name,
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    stddev_samp(inv_quantity_on_hand) stdev,
    avg(inv_quantity_on_hand) mean
  FROM ${database}.inventory, ${database}.item, ${database}.warehouse, ${database}.date_dim
  WHERE inv_item_sk = i_item_sk
    AND inv_warehouse_sk = w_warehouse_sk
    AND inv_date_sk = d_date_sk
    AND d_year = 2001
  GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
  WHERE CASE mean
        WHEN 0
          THEN 0
        ELSE stdev / mean END > 1)
SELECT
  inv1.w_warehouse_sk as w_warehouse_sk_1,
  inv1.i_item_sk as i_item_sk_1,
  inv1.d_moy as d_moy_1,
  inv1.mean as mean_1,
  inv1.cov as cov_1,
  inv2.w_warehouse_sk as w_warehouse_sk_2,
  inv2.i_item_sk as i_item_sk_2,
  inv2.d_moy as d_moy_2,
  inv2.mean as mean_2,
  inv2.cov as cov_2
FROM inv inv1, inv inv2
WHERE inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov
  , inv2.d_moy, inv2.mean, inv2.cov

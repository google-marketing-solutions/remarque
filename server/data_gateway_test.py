import pytest
import data_gateway
import config


def test_convert_duration_to_interval() -> None:
  cfg = config.Config()
  dg = data_gateway.DataGateway(cfg, None, None)

  expr = dg._convert_duration_to_interval('30d')
  assert expr == 'DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'

  expr = dg._convert_duration_to_interval('3M')
  assert expr == 'DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)'

  expr = dg._convert_duration_to_interval('2Y')
  assert expr == 'DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)'

  expr = dg._convert_duration_to_interval('1M10D')
  assert expr == 'DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), INTERVAL 10 DAY)'

  expr = dg._convert_duration_to_interval('1Y6M')
  assert expr == 'DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 6 MONTH)'

  expr = dg._convert_duration_to_interval('1y6m10d')
  assert expr == 'DATE_SUB(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 6 MONTH), INTERVAL 10 DAY)'

  # invalid
  with pytest.raises(ValueError):
    dg._convert_duration_to_interval('1')
  with pytest.raises(ValueError):
    dg._convert_duration_to_interval('1z')
  with pytest.raises(ValueError):
    dg._convert_duration_to_interval('')
  with pytest.raises(ValueError):
    dg._convert_duration_to_interval('10 y')

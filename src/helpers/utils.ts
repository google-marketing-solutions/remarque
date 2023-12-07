import format from 'date-fns/format';

export function formatArray(val: any) {
  if (val && val.length) {
    return val.join(', ');
  }
  return val;
}

export function formatDate(val: Date, includeTime = false) {
  //let ret = val.toISOString().substring(0, '2000-01-01'.length);
  if (typeof val === 'string') {
    val = new Date(val);
  }
  if (includeTime) {
    return format(val, 'yyyy.MM.dd HH:mm:ss');
  }
  return format(val, 'yyyy.MM.dd');
}

export function formatFloat(val: number | undefined, digits = 5) {
  val = Number.parseFloat(<any>val);
  return val >= 0 ? val.toFixed(digits) : '-';
}

export function isFinite(value: any) {
  return typeof value == 'number' && Number.isFinite(value);
}

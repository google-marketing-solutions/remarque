export function formatArray(val: any) {
  if (val && val.length) {
    return val.join(', ');
  }
  return val;
}

export function formatDate(val: Date) {
  return val.toISOString().substring(0, '2000-01-01'.length);
}

export function formatFloat(val: number | undefined) {
  return val ? val.toFixed(5) : '-'
}

// k6 gRPC load test: each iteration does one Put and one Get on the
// current leader. Run with ./scripts/k6.sh (uses the grafana/k6 image).
import grpc from 'k6/net/grpc';
import { check } from 'k6';

const NODES = (__ENV.NODES || 'node1:9001,node2:9002,node3:9003').split(',');
const client = new grpc.Client();
client.load(['/proto'], 'kv/v1/kv.proto');

export const options = {
  scenarios: {
    steady: { executor: 'constant-vus', vus: Number(__ENV.VUS || 32), duration: __ENV.DURATION || '30s' },
  },
  thresholds: {
    checks: ['rate>0.999'],
    'grpc_req_duration{name:kv.v1.KV/Put}': ['p(95)<100'],
    'grpc_req_duration{name:kv.v1.KV/Get}': ['p(95)<50'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

// Find the leader once (Status is served by every node).
export function setup() {
  for (const addr of NODES) {
    try {
      client.connect(addr, { plaintext: true, timeout: '2s' });
      const r = client.invoke('kv.v1.KV/Status', {});
      client.close();
      if (r.status === grpc.StatusOK && r.message.state === 'Leader') return { leader: addr };
    } catch (e) { /* node down: try the next one */ }
  }
  throw new Error('no leader found');
}

export default function (data) {
  if (__ITER === 0) client.connect(data.leader, { plaintext: true });
  const key = `k6-${__VU}-${__ITER % 1000}`;
  const put = client.invoke('kv.v1.KV/Put', { key: key, value: `v-${__ITER}` });
  check(put, { 'put ok': (r) => r.status === grpc.StatusOK });
  if (put.status !== grpc.StatusOK && !globalThis.logged) { globalThis.logged = true; console.log(`VU ${__VU} put failed: status=${put.status} ${JSON.stringify(put.error)} leader=${data.leader}`); }
  const get = client.invoke('kv.v1.KV/Get', { key: key });
  check(get, { 'get ok': (r) => r.status === grpc.StatusOK && r.message.value === `v-${__ITER}` });
}

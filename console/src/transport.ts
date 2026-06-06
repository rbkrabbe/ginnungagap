/// <reference types="vite/client" />
import { createGrpcWebTransport } from "@connectrpc/connect-web";

export const clientTransport = createGrpcWebTransport({
  baseUrl: (import.meta.env.VITE_GGAP_CLIENT_URL as string) ?? "http://localhost:17000",
});

export const clusterTransport = createGrpcWebTransport({
  baseUrl: (import.meta.env.VITE_GGAP_CLUSTER_URL as string) ?? "http://localhost:17001",
});

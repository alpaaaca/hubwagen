// The Connect clients the UI talks to. The UI is served by the server it
// talks to, so requests go to its own origin; tests provide an in-memory
// transport instead (ApiProvider).
import { createClient, type Client, type Transport } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { createContext, useContext, type ReactNode } from "react";

import { CatalogService } from "./gen/hubwagen/v1/catalog_pb";
import { HaulService } from "./gen/hubwagen/v1/haul_pb";
import { LogService } from "./gen/hubwagen/v1/log_pb";

export type Api = {
  hauls: Client<typeof HaulService>;
  catalog: Client<typeof CatalogService>;
  logs: Client<typeof LogService>;
};

export function apiFor(transport: Transport): Api {
  return {
    hauls: createClient(HaulService, transport),
    catalog: createClient(CatalogService, transport),
    logs: createClient(LogService, transport),
  };
}

const ApiContext = createContext<Api | null>(null);

export function ApiProvider({ api, children }: { api?: Api; children: ReactNode }) {
  const value = api ?? apiFor(createConnectTransport({ baseUrl: window.location.origin }));
  return <ApiContext.Provider value={value}>{children}</ApiContext.Provider>;
}

export function useApi(): Api {
  const api = useContext(ApiContext);
  if (!api) throw new Error("useApi outside ApiProvider");
  return api;
}

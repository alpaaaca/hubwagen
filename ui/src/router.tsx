// A hash router: #/hauls (the pipelines), #/pipelines/<haul name>,
// #/hauls/<id> (one run), #/hauls/<id>/<seq> (one run, one step's logs). Hash routes need nothing from the
// server, which serves index.html for every path anyway.
import { useEffect, useState, type MouseEvent, type ReactNode } from "react";

export type Route = { name: "hauls" } | { name: "pipeline"; haul: string } | { name: "haul"; id: string; step?: number };

export function parse(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  if (parts[0] === "hauls" && parts[1]) return { name: "haul", id: parts[1], step: Number(parts[2]) || undefined };
  if (parts[0] === "pipelines" && parts[1]) return { name: "pipeline", haul: parts[1] };
  return { name: "hauls" };
}

export function href(r: Route): string {
  switch (r.name) {
    case "haul":
      return `#/hauls/${encodeURIComponent(r.id)}` + (r.step ? `/${r.step}` : "");
    case "pipeline":
      return `#/pipelines/${encodeURIComponent(r.haul)}`;
    default:
      return "#/hauls";
  }
}

export function navigate(r: Route) {
  window.location.hash = href(r);
}

export function useRoute(): Route {
  const [route, setRoute] = useState(() => parse(window.location.hash));
  useEffect(() => {
    const on = () => setRoute(parse(window.location.hash));
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return route;
}

type LinkProps = { to: Route; children: ReactNode; className?: string; label?: string; onClick?: (e: MouseEvent) => void };

export function Link({ to, children, className, label, onClick }: LinkProps) {
  return (
    <a href={href(to)} className={className} aria-label={label} title={label} onClick={onClick}>
      {children}
    </a>
  );
}

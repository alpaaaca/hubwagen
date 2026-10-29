import { useRoute } from "./router";
import { HaulDetail } from "./views/HaulDetail";
import { HaulList } from "./views/HaulList";
import { Pipeline } from "./views/Pipeline";

export function App() {
  const route = useRoute();
  return (
    <>
      <header>
        <a href="#/hauls" className="brand">
          hubwagen
        </a>
        <nav>
          <a href="#/hauls" className="active">
            Pipelines
          </a>
        </nav>
      </header>
      <main>
        {route.name === "haul" ? <HaulDetail id={route.id} /> : route.name === "pipeline" ? <Pipeline haul={route.haul} /> : <HaulList />}
      </main>
    </>
  );
}

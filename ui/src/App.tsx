import { useRoute } from "./router";
import { RunDetail } from "./views/RunDetail";
import { RunList } from "./views/RunList";
import { Pipeline } from "./views/Pipeline";

export function App() {
  const route = useRoute();
  return (
    <>
      <header>
        <a href="#/runs" className="brand">
          moksha
        </a>
        <nav>
          <a href="#/runs" className="active">
            Pipelines
          </a>
        </nav>
      </header>
      <main>
        {route.name === "run" ? <RunDetail key={route.id} id={route.id} step={route.step} /> : route.name === "pipeline" ? <Pipeline run={route.run} /> : <RunList />}
      </main>
    </>
  );
}

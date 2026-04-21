(function () {
  const GRAPH_ID = "globe-graph";
  const ROTATE_STEP = Math.PI / 36; // ~5 degrees
  const TILT_STEP = Math.PI / 72; // ~2.5 degrees
  const MIN_POLAR = 0.12;
  const MAX_POLAR = Math.PI - 0.12;

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function toSpherical(eye) {
    const x = Number(eye.x ?? 1.5);
    const y = Number(eye.y ?? 1.5);
    const z = Number(eye.z ?? 0.8);

    const r = Math.max(Math.sqrt(x * x + y * y + z * z), 1e-9);
    const theta = Math.atan2(y, x); // azimuth
    const phi = Math.acos(clamp(z / r, -1, 1)); // polar

    return { r, theta, phi };
  }

  function toCartesian(spherical) {
    const { r, theta, phi } = spherical;
    const sinPhi = Math.sin(phi);

    return {
      x: r * sinPhi * Math.cos(theta),
      y: r * sinPhi * Math.sin(theta),
      z: r * Math.cos(phi),
    };
  }

  function getGraphDiv() {
    const root = document.getElementById(GRAPH_ID);
    if (!root) return null;
    return root.querySelector(".js-plotly-plot") || root;
  }

  function getCurrentEye(gd) {
    const fallback = { x: 1.5, y: 1.5, z: 0.8 };
    return gd?._fullLayout?.scene?.camera?.eye || fallback;
  }

  function rotateCamera(event) {
    const key = event.key;
    if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(key)) {
      return;
    }

    const tag = (document.activeElement?.tagName || "").toUpperCase();
    if (["INPUT", "TEXTAREA", "SELECT"].includes(tag)) {
      return;
    }

    const gd = getGraphDiv();
    if (!gd || typeof window.Plotly?.relayout !== "function") {
      return;
    }

    event.preventDefault();

    const spherical = toSpherical(getCurrentEye(gd));

    if (key === "ArrowLeft") spherical.theta -= ROTATE_STEP;
    if (key === "ArrowRight") spherical.theta += ROTATE_STEP;
    if (key === "ArrowUp") spherical.phi = clamp(spherical.phi - TILT_STEP, MIN_POLAR, MAX_POLAR);
    if (key === "ArrowDown") spherical.phi = clamp(spherical.phi + TILT_STEP, MIN_POLAR, MAX_POLAR);

    const nextEye = toCartesian(spherical);
    window.Plotly.relayout(gd, {
      "scene.camera.eye": nextEye,
    });
  }

  window.addEventListener("keydown", rotateCamera, { passive: false });
})();

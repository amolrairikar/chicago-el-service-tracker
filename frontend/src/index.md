# CTA "El" Service Tracker

Welcome. This dashboard surfaces service metrics for Chicago's "El" trains,
built with [Observable Framework](https://observablehq.com/framework/) and hosted
on S3 + CloudFront.

It's a scaffold for now. The planned views filter by **date range** and **train
line** — a good fit for Observable's `Inputs` and `Plot`. Backend data will be
fetched at runtime from the same-origin `/data/*` path (proxied to the pipeline's
`gold/` layer) once a gold-writer publishes it.

```js
display(html`<div style="padding: 1rem; border: 1px solid var(--theme-foreground-faint); border-radius: 8px;">
  Scaffold is live — filtered dashboards coming soon.
</div>`);
```

## Next steps

- Publish browser-friendly JSON under the pipeline's <code>gold/</code> prefix.
- Fetch it here with <code>FileAttachment</code> or <code>fetch("/data/…")</code>.
- Add <code>Inputs.date</code> / <code>Inputs.select</code> filters and
  <code>Plot</code> charts.

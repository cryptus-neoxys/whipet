export function Footer() {
  return (
    <footer className="footer">
      <div>
        <strong>Whippet</strong> · Apache-2.0 · an independent showcase. Not affiliated with or
        endorsed by the Apache Software Foundation. Apache Iggy is a project of the ASF.
      </div>
      <div className="footer-links">
        <a href="https://iggy.apache.org" target="_blank" rel="noreferrer">
          Apache Iggy
        </a>
        <span>·</span>
        <a
          href="https://github.com/cryptus-neoxys/whippet"
          target="_blank"
          rel="noreferrer"
        >
          Source
        </a>
        <span>·</span>
        <span>M4/Docker numbers are dev-loop, never headline</span>
      </div>
    </footer>
  );
}

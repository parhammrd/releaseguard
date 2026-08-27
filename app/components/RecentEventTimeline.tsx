import type { TimelineEvent } from "@/app/types";

function formatTime(value: string) {
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function RecentEventTimeline({
  events,
  cloudMode,
}: {
  events: TimelineEvent[];
  cloudMode: boolean;
}) {
  return (
    <section className="panel timeline-card">
      <div className="panel-head">
        <div>
          <p className="eyebrow">RECENT EVENTS</p>
          <h3>{cloudMode ? "Cloud-mode event timeline" : "Local event timeline"}</h3>
        </div>
        <span className="stream-label">
          <i /> EVENT FEED
        </span>
      </div>
      <div className="timeline-list">
        {[...events]
          .reverse()
          .slice(0, 5)
          .map((event) => (
            <div
              className={`timeline-row timeline-${event.tone}`}
              key={event.event_id}
            >
              <span className="event-dot" />
              <time>{formatTime(event.occurred_at)}</time>
              <div>
                <strong>{event.title}</strong>
                <p>{event.detail}</p>
              </div>
              <b>{event.event_type.replaceAll("_", " ")}</b>
            </div>
          ))}
      </div>
    </section>
  );
}

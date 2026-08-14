import { Fragment, useCallback, useEffect, useState } from "react";

import {
  ApiError,
  actOnAcquisition,
  addAcquisitionMagnet,
  getAcquisition,
  getTrackerStatus,
  isUnauthorized,
  listAcquisitions,
} from "../api";
import { Notice } from "./Feedback";
import { Spinner } from "./Icons";
import type {
  Acquisition,
  AcquisitionDetail,
  AcquisitionState,
  MagnetAttempt,
  TrackerStatus,
} from "../types";

const STATE_LABELS: Record<AcquisitionState, string> = {
  discovered: "待解析",
  downloading: "下载中",
  archived: "已入库",
  resolve_failed: "未找到磁力",
  exhausted: "磁力已用尽",
  needs_attention: "待处理",
  ignored: "已忽略",
};

const ATTEMPT_LABELS: Record<string, string> = {
  pending: "待用",
  submitted: "已提交",
  downloading: "下载中",
  finished: "已完成",
  archiving: "归档中",
  archived: "已入库",
  junk: "无有效视频",
  error: "离线出错",
  stalled: "长期无进度",
  lost: "任务丢失",
};

/** Ordered so the states an operator must act on come first. */
const GROUPS: AcquisitionState[] = [
  "needs_attention",
  "exhausted",
  "resolve_failed",
  "downloading",
  "discovered",
  "archived",
  "ignored",
];

function formatTime(value: string | null): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function ProgressBar({ value }: { value: number | null }) {
  if (value === null) return <span className="acq-muted">—</span>;
  return (
    <span className="acq-progress" title={`${value.toFixed(1)}%`}>
      <span
        className="acq-progress-fill"
        style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
      />
      <span className="acq-progress-text">{value.toFixed(0)}%</span>
    </span>
  );
}

function AttemptRow({ attempt }: { attempt: MagnetAttempt }) {
  return (
    <tr>
      <td>#{attempt.attempt_no}</td>
      <td>{attempt.magnet_source}</td>
      <td>{ATTEMPT_LABELS[attempt.state] ?? attempt.state}</td>
      <td>
        <ProgressBar value={attempt.progress} />
      </td>
      <td className="acq-muted">{attempt.error ?? "—"}</td>
      <td className="acq-muted">{formatTime(attempt.updated_at)}</td>
    </tr>
  );
}

export function AcquisitionPanel({
  onUnauthorized,
}: {
  onUnauthorized: () => void;
}) {
  const [page, setPage] = useState<{
    items: Acquisition[];
    counts: Partial<Record<AcquisitionState, number>>;
  }>({
    items: [],
    counts: {},
  });
  const [tracker, setTracker] = useState<TrackerStatus | null>(null);
  const [filter, setFilter] = useState<AcquisitionState | null>(
    "needs_attention",
  );
  const [detail, setDetail] = useState<AcquisitionDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [magnet, setMagnet] = useState("");

  const refresh = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const [nextPage, nextTracker] = await Promise.all([
          listAcquisitions(filter, 50, signal),
          getTrackerStatus(signal),
        ]);
        setPage(nextPage);
        setTracker(nextTracker);
        setError(null);
      } catch (err) {
        if (signal?.aborted) return;
        if (isUnauthorized(err)) {
          onUnauthorized();
          return;
        }
        setError(err instanceof ApiError ? err.message : "加载下载追踪失败。");
      } finally {
        setLoading(false);
      }
    },
    [filter, onUnauthorized],
  );

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    void refresh(controller.signal);
    return () => controller.abort();
  }, [refresh]);

  const act = useCallback(
    async (avid: string, action: "retry" | "ignore" | "resume") => {
      setBusy(avid);
      setError(null);
      try {
        await actOnAcquisition(avid, action);
        await refresh();
        if (detail?.avid === avid) setDetail(await getAcquisition(avid));
      } catch (err) {
        if (isUnauthorized(err)) {
          onUnauthorized();
          return;
        }
        setError(err instanceof ApiError ? err.message : "操作失败。");
      } finally {
        setBusy(null);
      }
    },
    [detail, onUnauthorized, refresh],
  );

  const submitMagnet = useCallback(async () => {
    if (!detail || !magnet.trim()) return;
    setBusy(detail.avid);
    setError(null);
    try {
      await addAcquisitionMagnet(detail.avid, magnet.trim());
      setMagnet("");
      await refresh();
      setDetail(await getAcquisition(detail.avid));
    } catch (err) {
      if (isUnauthorized(err)) {
        onUnauthorized();
        return;
      }
      setError(err instanceof ApiError ? err.message : "提交磁力失败。");
    } finally {
      setBusy(null);
    }
  }, [detail, magnet, onUnauthorized, refresh]);

  const openDetail = useCallback(
    async (avid: string) => {
      if (detail?.avid === avid) {
        setDetail(null);
        return;
      }
      try {
        setDetail(await getAcquisition(avid));
      } catch (err) {
        if (isUnauthorized(err)) onUnauthorized();
      }
    },
    [detail, onUnauthorized],
  );

  return (
    <section className="panel dashboard-panel">
      <header className="panel-heading">
        <div>
          <h2>下载追踪</h2>
          <p className="acq-muted">
            每个番号从发现到入库的进度。下载出错、长期无进度、或下完发现没有有效视频时会自动换下一个磁力。
          </p>
        </div>
        <button
          type="button"
          className="button secondary"
          onClick={() => void refresh()}
          disabled={loading}
        >
          刷新
        </button>
      </header>

      {tracker && !tracker.running && (
        <Notice
          tone="warning"
          title="下载追踪未运行"
          body={tracker.reason ?? "尚未配置离线任务目录。"}
        />
      )}
      {tracker?.last_error && (
        <Notice tone="error" title="上次轮询出错" body={tracker.last_error} />
      )}
      {error && <Notice tone="error" title="下载追踪请求失败" body={error} />}

      <div className="stat-chips acq-filters">
        <button
          type="button"
          className={filter === null ? "stat-chip stat-chip-on" : "stat-chip"}
          onClick={() => setFilter(null)}
        >
          全部
        </button>
        {GROUPS.map((state) => (
          <button
            key={state}
            type="button"
            className={
              filter === state ? "stat-chip stat-chip-on" : "stat-chip"
            }
            onClick={() => setFilter(state)}
          >
            {STATE_LABELS[state]}
            {page.counts[state] ? (
              <span className="stat-chip-count">{page.counts[state]}</span>
            ) : null}
          </button>
        ))}
      </div>

      {tracker?.last_polled_at && (
        <p className="acq-muted">
          上次轮询：{formatTime(tracker.last_polled_at)}
        </p>
      )}

      {loading ? (
        <Spinner />
      ) : page.items.length === 0 ? (
        <p className="acq-muted">没有符合条件的番号。</p>
      ) : (
        <div className="run-table-wrap">
          <table className="run-table">
            <thead>
              <tr>
                <th>番号</th>
                <th>状态</th>
                <th>说明</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {page.items.map((item) => (
                <Fragment key={item.avid}>
                  <tr>
                    <td>
                      <button
                        type="button"
                        className="text-button"
                        onClick={() => void openDetail(item.avid)}
                      >
                        {item.avid}
                      </button>
                    </td>
                    <td>{STATE_LABELS[item.state]}</td>
                    <td className="acq-muted">{item.note ?? "—"}</td>
                    <td className="acq-muted">{formatTime(item.updated_at)}</td>
                    <td className="acq-actions">
                      {item.state === "needs_attention" && (
                        <button
                          type="button"
                          className="button secondary"
                          disabled={busy === item.avid}
                          onClick={() => void act(item.avid, "resume")}
                        >
                          已处理，继续
                        </button>
                      )}
                      {item.state !== "archived" &&
                        item.state !== "ignored" && (
                          <button
                            type="button"
                            className="button secondary"
                            disabled={busy === item.avid}
                            onClick={() => void act(item.avid, "retry")}
                          >
                            换下一个磁力
                          </button>
                        )}
                      {item.state !== "archived" &&
                        item.state !== "ignored" && (
                          <button
                            type="button"
                            className="button secondary"
                            disabled={busy === item.avid}
                            onClick={() => void act(item.avid, "ignore")}
                          >
                            忽略
                          </button>
                        )}
                    </td>
                  </tr>
                  {detail?.avid === item.avid && (
                    <tr>
                      <td colSpan={5}>
                        <div className="acq-detail">
                          <div className="run-table-wrap">
                            <table className="run-table">
                              <thead>
                                <tr>
                                  <th>尝试</th>
                                  <th>来源</th>
                                  <th>状态</th>
                                  <th>进度</th>
                                  <th>错误</th>
                                  <th>更新</th>
                                </tr>
                              </thead>
                              <tbody>
                                {detail.attempts.map((attempt) => (
                                  <AttemptRow
                                    key={attempt.attempt_no}
                                    attempt={attempt}
                                  />
                                ))}
                              </tbody>
                            </table>
                          </div>
                          {detail.archived_paths.length > 0 && (
                            <p className="acq-muted">
                              已入库：{detail.archived_paths.join("、")}
                            </p>
                          )}
                          {detail.state !== "archived" &&
                            detail.state !== "ignored" && (
                              <div className="acq-magnet">
                                <input
                                  type="text"
                                  value={magnet}
                                  placeholder="magnet:?xt=urn:btih:..."
                                  onChange={(event) =>
                                    setMagnet(event.target.value)
                                  }
                                />
                                <button
                                  type="button"
                                  className="button primary"
                                  disabled={
                                    busy === detail.avid || !magnet.trim()
                                  }
                                  onClick={() => void submitMagnet()}
                                >
                                  手动提交磁力
                                </button>
                              </div>
                            )}
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

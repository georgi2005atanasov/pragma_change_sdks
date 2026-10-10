export interface ActionContext {
  actor: string;
  action: string;
  session_id: string;
  event_id?: string;
  application_id?: string;
  resource?: string | null;
  workflow_id?: string | null;
  correlation_id?: string | null;
  timestamp_ms?: number;
  outcome?: "attempted" | "succeeded" | "failed";
  cohort?: string;
  sequence_start?: boolean;
  sequence_number?: number | null;
  attributes?: Record<string, string | number | boolean>;
}
export interface Decision {
  action: "allow" | "check" | "block";
  recommended_action: string;
  shadow: boolean;
  surprise: number | null;
  model_id: string | null;
  sequence_mean_surprise?: number | null;
  baseline?: string | null;
  evidence_count?: number;
  reasons: string[];
}
export class PragmaChange {
  constructor(config: {
    url: string;
    token: string;
    applicationId: string;
    allowedAttributes?: string[];
  });
  event(input: ActionContext): ActionContext;
  record(input: ActionContext): boolean;
  evaluate(input: ActionContext): Promise<Decision>;
  flush(): Promise<void>;
  close(): Promise<void>;
  health(): { queued: number; sent: number; dropped: number };
  middleware(
    resolve: (request: any) => ActionContext | null,
  ): (request: any, response: any, next: () => void) => void;
}
export interface RecordingConfig {
  format: "request_events" | "behavior_events";
  application_id?: string;
  pipeline_version_id?: string;
  schema_hash?: string;
  endpoint_id?: string;
  allowed_headers?: string[];
  allowed_query?: string[];
  allowed_body_fields?: string[];
  allowed_attributes?: string[];
  include_client_ip?: boolean;
  max_bytes?: number;
  max_records?: number;
}
export interface RequestRecord {
  timestamp_ms: number;
  endpoint_id: string;
  method: string;
  path: string;
  headers?: Record<string, string>;
  query?: Record<string, string>;
  body?: Record<string, string | number | boolean>;
  body_size: number;
  client_ip?: string;
}
export class DatasetRecorder {
  constructor(directory: string, config: RecordingConfig);
  record(input: ActionContext | RequestRecord): void;
  close(): unknown;
}

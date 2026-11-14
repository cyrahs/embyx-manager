//! The OpenAPI operation objects of the /api/merge routes, as the baseline's document has them
//! (`goldens/api/openapi_main.json`) with `$ref`s inlined, the documentation-only keys left out and
//! FastAPI's 422 response added by [`crate::openapi::operation`]. Generated from the
//! baseline's document; the structural test compares the two.

/// Method, path below the router's prefix, and operation object of each route.
const OPERATIONS: &[(&str, &str, &str)] = &[
    (
        "GET",
        "/titles",
        r#"{
 "parameters": [
  {
   "name": "refresh",
   "in": "query",
   "required": false,
   "schema": {
    "type": "boolean",
    "default": false
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "items": {
        "items": {
         "properties": {
          "avid": {
           "type": "string"
          },
          "directory": {
           "type": "string"
          },
          "part_count": {
           "type": "integer"
          },
          "parts": {
           "items": {
            "type": "integer"
           },
           "type": "array"
          },
          "missing": {
           "items": {
            "type": "integer"
           },
           "type": "array"
          },
          "library_dir": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "brand": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "problem": {
           "anyOf": [
            {
             "type": "string",
             "enum": [
              "unreadable_strm",
              "outside_library",
              "scattered_parts",
              "whole_copy"
             ]
            },
            {
             "type": "null"
            }
           ]
          },
          "source": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "source_basis": {
           "anyOf": [
            {
             "type": "string",
             "enum": [
              "library",
              "ledger"
             ]
            },
            {
             "type": "null"
            }
           ]
          },
          "stackable": {
           "type": "boolean"
          },
          "mergeable": {
           "type": "boolean"
          }
         },
         "type": "object",
         "required": [
          "avid",
          "directory",
          "part_count",
          "parts",
          "missing",
          "library_dir",
          "brand",
          "problem",
          "source",
          "source_basis",
          "stackable",
          "mergeable"
         ]
        },
        "type": "array"
       },
       "routes": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "scanned_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "reason": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       }
      },
      "type": "object",
      "required": [
       "items",
       "routes",
       "scanned_at"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "GET",
        "/tasks",
        r#"{
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "items": {
        "items": {
         "properties": {
          "id": {
           "type": "integer"
          },
          "avid": {
           "type": "string"
          },
          "source": {
           "type": "string"
          },
          "library_dir": {
           "type": "string"
          },
          "part_count": {
           "type": "integer"
          },
          "state": {
           "type": "string",
           "enum": [
            "queued",
            "transcoding",
            "merging",
            "uploading",
            "verifying",
            "replacing",
            "archiving",
            "done",
            "failed",
            "cancelled"
           ]
          },
          "failed_state": {
           "anyOf": [
            {
             "type": "string",
             "enum": [
              "queued",
              "transcoding",
              "merging",
              "uploading",
              "verifying",
              "replacing",
              "archiving",
              "done",
              "failed",
              "cancelled"
             ]
            },
            {
             "type": "null"
            }
           ]
          },
          "phase": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "progress": {
           "anyOf": [
            {
             "type": "number"
            },
            {
             "type": "null"
            }
           ]
          },
          "merged_bytes": {
           "anyOf": [
            {
             "type": "integer"
            },
            {
             "type": "null"
            }
           ]
          },
          "uploaded_bytes": {
           "anyOf": [
            {
             "type": "integer"
            },
            {
             "type": "null"
            }
           ]
          },
          "upload_attempts": {
           "type": "integer"
          },
          "error": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "notice": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "created_at": {
           "type": "string",
           "format": "date-time"
          },
          "updated_at": {
           "type": "string",
           "format": "date-time"
          },
          "finished_at": {
           "anyOf": [
            {
             "type": "string",
             "format": "date-time"
            },
            {
             "type": "null"
            }
           ]
          },
          "cancellable": {
           "type": "boolean"
          },
          "retryable": {
           "type": "boolean"
          },
          "transcoder": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "home": {
           "type": "boolean",
           "default": false
          },
          "transcode": {
           "anyOf": [
            {
             "additionalProperties": true,
             "type": "object"
            },
            {
             "type": "null"
            }
           ]
          },
          "transcode_paused": {
           "type": "boolean",
           "default": false
          }
         },
         "type": "object",
         "required": [
          "id",
          "avid",
          "source",
          "library_dir",
          "part_count",
          "state",
          "failed_state",
          "phase",
          "progress",
          "merged_bytes",
          "uploaded_bytes",
          "upload_attempts",
          "error",
          "notice",
          "created_at",
          "updated_at",
          "finished_at",
          "cancellable",
          "retryable"
         ]
        },
        "type": "array"
       },
       "unavailable": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "auto": {
        "anyOf": [
         {
          "properties": {
           "state": {
            "type": "string",
            "enum": [
             "off",
             "unavailable",
             "busy",
             "paused",
             "idle"
            ]
           },
           "room": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "skipped": {
            "items": {
             "properties": {
              "avid": {
               "type": "string"
              },
              "size": {
               "anyOf": [
                {
                 "type": "integer"
                },
                {
                 "type": "null"
                }
               ]
              },
              "reason": {
               "type": "string"
              }
             },
             "type": "object",
             "required": [
              "avid",
              "size",
              "reason"
             ]
            },
            "type": "array"
           },
           "skipped_count": {
            "type": "integer",
            "default": 0
           },
           "checked_at": {
            "anyOf": [
             {
              "type": "string",
              "format": "date-time"
             },
             {
              "type": "null"
             }
            ]
           }
          },
          "type": "object",
          "required": [
           "state"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "worker": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       }
      },
      "type": "object",
      "required": [
       "items",
       "unavailable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "avid": {
       "type": "string",
       "maxLength": 64,
       "minLength": 1
      },
      "source": {
       "anyOf": [
        {
         "type": "string",
         "maxLength": 64
        },
        {
         "type": "null"
        }
       ]
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "avid"
     ]
    }
   }
  },
  "required": true
 },
 "responses": {
  "201": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 },
 "security": [
  {
   "HTTPBearer": []
  }
 ]
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/pause",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/resume",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/use-cpu",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/cancel",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/retry",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "id": {
        "type": "integer"
       },
       "avid": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "library_dir": {
        "type": "string"
       },
       "part_count": {
        "type": "integer"
       },
       "state": {
        "type": "string",
        "enum": [
         "queued",
         "transcoding",
         "merging",
         "uploading",
         "verifying",
         "replacing",
         "archiving",
         "done",
         "failed",
         "cancelled"
        ]
       },
       "failed_state": {
        "anyOf": [
         {
          "type": "string",
          "enum": [
           "queued",
           "transcoding",
           "merging",
           "uploading",
           "verifying",
           "replacing",
           "archiving",
           "done",
           "failed",
           "cancelled"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "phase": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "progress": {
        "anyOf": [
         {
          "type": "number"
         },
         {
          "type": "null"
         }
        ]
       },
       "merged_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "uploaded_bytes": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "upload_attempts": {
        "type": "integer"
       },
       "error": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "notice": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       },
       "finished_at": {
        "anyOf": [
         {
          "type": "string",
          "format": "date-time"
         },
         {
          "type": "null"
         }
        ]
       },
       "cancellable": {
        "type": "boolean"
       },
       "retryable": {
        "type": "boolean"
       },
       "transcoder": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "home": {
        "type": "boolean",
        "default": false
       },
       "transcode": {
        "anyOf": [
         {
          "additionalProperties": true,
          "type": "object"
         },
         {
          "type": "null"
         }
        ]
       },
       "transcode_paused": {
        "type": "boolean",
        "default": false
       }
      },
      "type": "object",
      "required": [
       "id",
       "avid",
       "source",
       "library_dir",
       "part_count",
       "state",
       "failed_state",
       "phase",
       "progress",
       "merged_bytes",
       "uploaded_bytes",
       "upload_attempts",
       "error",
       "notice",
       "created_at",
       "updated_at",
       "finished_at",
       "cancellable",
       "retryable"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
];

/// The operation object of one route, by method and path below the router's prefix.
pub(crate) fn operation(method: &str, path: &str) -> &'static str {
    let Some((_, _, text)) = OPERATIONS.iter().find(|(m, p, _)| *m == method && *p == path) else {
        panic!("no operation {method} {path}");
    };
    text
}

//! The OpenAPI operation objects of the playlists routes, as the baseline's document has them
//! (`goldens/api/openapi_main.json`) with `$ref`s inlined, the documentation-only keys left
//! out and FastAPI's 422 response added by [`crate::openapi::operation`]. Transcribed
//! from the baseline's pydantic models; the structural test compares the two documents.

/// Method, path below the router's prefix, and operation object of each route.
const OPERATIONS: &[(&str, &str, &str)] = &[
    (
        "GET",
        "",
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
          "key": {
           "type": "string"
          },
          "kind": {
           "type": "integer"
          },
          "note": {
           "type": "string"
          },
          "name": {
           "type": "string"
          },
          "enabled": {
           "type": "boolean"
          },
          "total": {
           "type": "integer"
          },
          "present": {
           "type": "integer"
          },
          "missing": {
           "type": "integer"
          },
          "emby_playlist_id": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "last_synced_at": {
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
          "last_error": {
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
          "key",
          "kind",
          "note",
          "name",
          "enabled",
          "total",
          "present",
          "missing",
          "emby_playlist_id",
          "last_synced_at",
          "last_error"
         ]
        },
        "type": "array"
       },
       "source": {
        "anyOf": [
         {
          "properties": {
           "database_name": {
            "type": "string"
           },
           "fetched_at": {
            "type": "string",
            "format": "date-time"
           }
          },
          "type": "object",
          "required": [
           "database_name",
           "fetched_at"
          ]
         },
         {
          "type": "null"
         }
        ]
       },
       "fill_task_dir": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "fill_reason": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "fills": {
        "items": {
         "properties": {
          "key": {
           "type": "string"
          },
          "task_dir_path": {
           "type": "string"
          },
          "state": {
           "type": "string",
           "enum": [
            "running",
            "done",
            "failed"
           ]
          },
          "done": {
           "type": "integer"
          },
          "total": {
           "type": "integer"
          },
          "items": {
           "items": {
            "properties": {
             "avid": {
              "type": "string"
             },
             "outcome": {
              "type": "string"
             }
            },
            "type": "object",
            "required": [
             "avid",
             "outcome"
            ]
           },
           "type": "array"
          },
          "counts": {
           "additionalProperties": {
            "type": "integer"
           },
           "type": "object"
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
          "started_at": {
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
          }
         },
         "type": "object",
         "required": [
          "key",
          "task_dir_path",
          "state",
          "done",
          "total",
          "items",
          "counts",
          "started_at"
         ]
        },
        "type": "array",
        "default": []
       }
      },
      "type": "object",
      "required": [
       "items",
       "source"
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
        "/{key}/missing",
        r#"{
 "parameters": [
  {
   "name": "key",
   "in": "path",
   "required": true,
   "schema": {
    "type": "string"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "key": {
        "type": "string"
       },
       "name": {
        "type": "string"
       },
       "items": {
        "items": {
         "properties": {
          "rank": {
           "type": "integer"
          },
          "avid": {
           "type": "string"
          },
          "title": {
           "type": "string"
          },
          "tracked": {
           "anyOf": [
            {
             "type": "string",
             "enum": [
              "discovered",
              "downloading",
              "archived",
              "resolve_failed",
              "exhausted",
              "needs_attention",
              "ignored"
             ]
            },
            {
             "type": "null"
            }
           ]
          }
         },
         "type": "object",
         "required": [
          "rank",
          "avid",
          "title",
          "tracked"
         ]
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "key",
       "name",
       "items"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "PATCH",
        "/{key}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "key",
   "in": "path",
   "required": true,
   "schema": {
    "type": "string"
   }
  }
 ],
 "requestBody": {
  "required": true,
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "enabled": {
       "type": "boolean"
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "enabled"
     ]
    }
   }
  }
 },
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "key": {
        "type": "string"
       },
       "kind": {
        "type": "integer"
       },
       "note": {
        "type": "string"
       },
       "name": {
        "type": "string"
       },
       "enabled": {
        "type": "boolean"
       },
       "total": {
        "type": "integer"
       },
       "present": {
        "type": "integer"
       },
       "missing": {
        "type": "integer"
       },
       "emby_playlist_id": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "last_synced_at": {
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
       "last_error": {
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
       "key",
       "kind",
       "note",
       "name",
       "enabled",
       "total",
       "present",
       "missing",
       "emby_playlist_id",
       "last_synced_at",
       "last_error"
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
        "/{key}/fill",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "key",
   "in": "path",
   "required": true,
   "schema": {
    "type": "string"
   }
  }
 ],
 "responses": {
  "202": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "key": {
        "type": "string"
       },
       "task_dir_path": {
        "type": "string"
       },
       "state": {
        "type": "string",
        "enum": [
         "running",
         "done",
         "failed"
        ]
       },
       "done": {
        "type": "integer"
       },
       "total": {
        "type": "integer"
       },
       "items": {
        "items": {
         "properties": {
          "avid": {
           "type": "string"
          },
          "outcome": {
           "type": "string"
          }
         },
         "type": "object",
         "required": [
          "avid",
          "outcome"
         ]
        },
        "type": "array"
       },
       "counts": {
        "additionalProperties": {
         "type": "integer"
        },
        "type": "object"
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
       "started_at": {
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
       }
      },
      "type": "object",
      "required": [
       "key",
       "task_dir_path",
       "state",
       "done",
       "total",
       "items",
       "counts",
       "started_at"
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
        "/{key}/fill",
        r#"{
 "parameters": [
  {
   "name": "key",
   "in": "path",
   "required": true,
   "schema": {
    "type": "string"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "key": {
        "type": "string"
       },
       "task_dir_path": {
        "type": "string"
       },
       "state": {
        "type": "string",
        "enum": [
         "running",
         "done",
         "failed"
        ]
       },
       "done": {
        "type": "integer"
       },
       "total": {
        "type": "integer"
       },
       "items": {
        "items": {
         "properties": {
          "avid": {
           "type": "string"
          },
          "outcome": {
           "type": "string"
          }
         },
         "type": "object",
         "required": [
          "avid",
          "outcome"
         ]
        },
        "type": "array"
       },
       "counts": {
        "additionalProperties": {
         "type": "integer"
        },
        "type": "object"
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
       "started_at": {
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
       }
      },
      "type": "object",
      "required": [
       "key",
       "task_dir_path",
       "state",
       "done",
       "total",
       "items",
       "counts",
       "started_at"
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

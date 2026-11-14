//! The OpenAPI operation objects of the monitor routes, as the baseline's document has them
//! (`goldens/api/openapi_main.json`) with `$ref`s inlined, the documentation-only keys left
//! out and FastAPI's 422 response added by [`crate::openapi::operation`]. Transcribed
//! from the baseline's pydantic models; the structural test compares the two documents.

/// Method, path below the router's prefix, and operation object of each route.
const OPERATIONS: &[(&str, &str, &str)] = &[
    (
        "GET",
        "/subscriptions",
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
          "kind": {
           "type": "string"
          },
          "category": {
           "type": "string"
          },
          "enabled": {
           "type": "boolean"
          },
          "url": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "feed_url": {
           "type": "string"
          },
          "talent_id": {
           "anyOf": [
            {
             "type": "integer"
            },
            {
             "type": "null"
            }
           ]
          },
          "name": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "aliases": {
           "items": {
            "type": "string"
           },
           "type": "array"
          },
          "seed_pending": {
           "type": "boolean"
          },
          "cursor_size": {
           "type": "integer"
          },
          "last_polled_at": {
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
          },
          "created_at": {
           "type": "string",
           "format": "date-time"
          },
          "updated_at": {
           "type": "string",
           "format": "date-time"
          }
         },
         "type": "object",
         "required": [
          "id",
          "kind",
          "category",
          "enabled",
          "url",
          "feed_url",
          "talent_id",
          "name",
          "aliases",
          "seed_pending",
          "cursor_size",
          "last_polled_at",
          "last_error",
          "created_at",
          "updated_at"
         ]
        },
        "type": "array"
       },
       "categories": {
        "items": {
         "type": "string"
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "items",
       "categories"
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
        "/subscriptions",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "kind": {
       "type": "string",
       "default": "rss"
      },
      "category": {
       "type": "string"
      },
      "name": {
       "anyOf": [
        {
         "type": "string"
        },
        {
         "type": "null"
        }
       ]
      },
      "url": {
       "anyOf": [
        {
         "type": "string"
        },
        {
         "type": "null"
        }
       ]
      },
      "talent_id": {
       "anyOf": [
        {
         "type": "integer"
        },
        {
         "type": "null"
        }
       ]
      },
      "aliases": {
       "items": {
        "type": "string"
       },
       "type": "array",
       "default": []
      },
      "seed": {
       "type": "boolean",
       "default": false
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "category"
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
       "kind": {
        "type": "string"
       },
       "category": {
        "type": "string"
       },
       "enabled": {
        "type": "boolean"
       },
       "url": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "feed_url": {
        "type": "string"
       },
       "talent_id": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "name": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "aliases": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "seed_pending": {
        "type": "boolean"
       },
       "cursor_size": {
        "type": "integer"
       },
       "last_polled_at": {
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
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       }
      },
      "type": "object",
      "required": [
       "id",
       "kind",
       "category",
       "enabled",
       "url",
       "feed_url",
       "talent_id",
       "name",
       "aliases",
       "seed_pending",
       "cursor_size",
       "last_polled_at",
       "last_error",
       "created_at",
       "updated_at"
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
        "PATCH",
        "/subscriptions/{subscription_id}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "subscription_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
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
       "anyOf": [
        {
         "type": "boolean"
        },
        {
         "type": "null"
        }
       ]
      },
      "category": {
       "anyOf": [
        {
         "type": "string"
        },
        {
         "type": "null"
        }
       ]
      },
      "url": {
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
     "additionalProperties": false,
     "type": "object"
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
       "id": {
        "type": "integer"
       },
       "kind": {
        "type": "string"
       },
       "category": {
        "type": "string"
       },
       "enabled": {
        "type": "boolean"
       },
       "url": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "feed_url": {
        "type": "string"
       },
       "talent_id": {
        "anyOf": [
         {
          "type": "integer"
         },
         {
          "type": "null"
         }
        ]
       },
       "name": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "aliases": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "seed_pending": {
        "type": "boolean"
       },
       "cursor_size": {
        "type": "integer"
       },
       "last_polled_at": {
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
       },
       "created_at": {
        "type": "string",
        "format": "date-time"
       },
       "updated_at": {
        "type": "string",
        "format": "date-time"
       }
      },
      "type": "object",
      "required": [
       "id",
       "kind",
       "category",
       "enabled",
       "url",
       "feed_url",
       "talent_id",
       "name",
       "aliases",
       "seed_pending",
       "cursor_size",
       "last_polled_at",
       "last_error",
       "created_at",
       "updated_at"
      ]
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "DELETE",
        "/subscriptions/{subscription_id}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "subscription_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "204": {}
 }
}"#,
    ),
    (
        "GET",
        "/status",
        r#"{
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "items": {
       "properties": {
        "pipeline": {
         "type": "string"
        },
        "enabled": {
         "type": "boolean"
        },
        "configured": {
         "type": "boolean"
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
        },
        "running_run_id": {
         "anyOf": [
          {
           "type": "string"
          },
          {
           "type": "null"
          }
         ]
        },
        "next_scheduled_at": {
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
        "last_run": {
         "anyOf": [
          {
           "properties": {
            "run_id": {
             "type": "string"
            },
            "pipeline": {
             "type": "string"
            },
            "trigger": {
             "type": "string"
            },
            "state": {
             "type": "string"
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
            },
            "stats": {
             "additionalProperties": {
              "type": "integer"
             },
             "type": "object"
            },
            "error_count": {
             "type": "integer"
            }
           },
           "type": "object",
           "required": [
            "run_id",
            "pipeline",
            "trigger",
            "state",
            "started_at",
            "finished_at",
            "stats",
            "error_count"
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
        "pipeline",
        "enabled",
        "configured",
        "reason",
        "running_run_id",
        "next_scheduled_at",
        "last_run"
       ]
      },
      "type": "array"
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/{pipeline}/trigger",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "pipeline",
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
       "run_id": {
        "type": "string"
       }
      },
      "type": "object",
      "required": [
       "run_id"
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
        "/{pipeline}/cancel",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "pipeline",
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
      "type": "object",
      "additionalProperties": {
       "type": "boolean"
      }
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "GET",
        "/runs",
        r#"{
 "parameters": [
  {
   "name": "pipeline",
   "in": "query",
   "required": false,
   "schema": {
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
  {
   "name": "limit",
   "in": "query",
   "required": false,
   "schema": {
    "type": "integer",
    "default": 20
   }
  },
  {
   "name": "offset",
   "in": "query",
   "required": false,
   "schema": {
    "type": "integer",
    "default": 0
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "type": "array",
      "items": {
       "properties": {
        "run_id": {
         "type": "string"
        },
        "pipeline": {
         "type": "string"
        },
        "trigger": {
         "type": "string"
        },
        "state": {
         "type": "string"
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
        },
        "stats": {
         "additionalProperties": {
          "type": "integer"
         },
         "type": "object"
        },
        "error_count": {
         "type": "integer"
        }
       },
       "type": "object",
       "required": [
        "run_id",
        "pipeline",
        "trigger",
        "state",
        "started_at",
        "finished_at",
        "stats",
        "error_count"
       ]
      }
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "GET",
        "/runs/{run_id}",
        r#"{
 "parameters": [
  {
   "name": "run_id",
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
       "run_id": {
        "type": "string"
       },
       "pipeline": {
        "type": "string"
       },
       "trigger": {
        "type": "string"
       },
       "state": {
        "type": "string"
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
       },
       "stats": {
        "additionalProperties": {
         "type": "integer"
        },
        "type": "object"
       },
       "error_count": {
        "type": "integer"
       },
       "errors": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "log_tail": {
        "items": {
         "type": "string"
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "run_id",
       "pipeline",
       "trigger",
       "state",
       "started_at",
       "finished_at",
       "stats",
       "error_count",
       "errors",
       "log_tail"
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
        "/acquisitions",
        r#"{
 "parameters": [
  {
   "name": "state",
   "in": "query",
   "required": false,
   "schema": {
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
  {
   "name": "limit",
   "in": "query",
   "required": false,
   "schema": {
    "type": "integer",
    "default": 50
   }
  },
  {
   "name": "offset",
   "in": "query",
   "required": false,
   "schema": {
    "type": "integer",
    "default": 0
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
          "state": {
           "type": "string"
          },
          "source": {
           "type": "string"
          },
          "note": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "archived_paths": {
           "items": {
            "type": "string"
           },
           "type": "array"
          },
          "next_action_at": {
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
          "release_date": {
           "anyOf": [
            {
             "type": "string",
             "format": "date"
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
          "replacing": {
           "items": {
            "type": "string"
           },
           "type": "array",
           "default": []
          }
         },
         "type": "object",
         "required": [
          "avid",
          "state",
          "source",
          "note",
          "archived_paths",
          "next_action_at",
          "release_date",
          "created_at",
          "updated_at"
         ]
        },
        "type": "array"
       },
       "counts": {
        "additionalProperties": {
         "type": "integer"
        },
        "type": "object"
       }
      },
      "type": "object",
      "required": [
       "items",
       "counts"
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
        "/acquisitions/{avid}",
        r#"{
 "parameters": [
  {
   "name": "avid",
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
       "avid": {
        "type": "string"
       },
       "state": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "note": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "archived_paths": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "next_action_at": {
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
       "release_date": {
        "anyOf": [
         {
          "type": "string",
          "format": "date"
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
       "replacing": {
        "items": {
         "type": "string"
        },
        "type": "array",
        "default": []
       },
       "attempts": {
        "items": {
         "properties": {
          "attempt_no": {
           "type": "integer"
          },
          "magnet_source": {
           "type": "string"
          },
          "state": {
           "type": "string"
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
          "submitted_at": {
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
          "updated_at": {
           "type": "string",
           "format": "date-time"
          },
          "info_hash": {
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
          "attempt_no",
          "magnet_source",
          "state",
          "progress",
          "error",
          "submitted_at",
          "updated_at",
          "info_hash"
         ]
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "avid",
       "state",
       "source",
       "note",
       "archived_paths",
       "next_action_at",
       "release_date",
       "created_at",
       "updated_at",
       "attempts"
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
        "/acquisitions/{avid}/retry",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "avid",
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
       "avid": {
        "type": "string"
       },
       "state": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "note": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "archived_paths": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "next_action_at": {
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
       "release_date": {
        "anyOf": [
         {
          "type": "string",
          "format": "date"
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
       "replacing": {
        "items": {
         "type": "string"
        },
        "type": "array",
        "default": []
       }
      },
      "type": "object",
      "required": [
       "avid",
       "state",
       "source",
       "note",
       "archived_paths",
       "next_action_at",
       "release_date",
       "created_at",
       "updated_at"
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
        "/acquisitions/{avid}/magnet",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "avid",
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
      "magnet": {
       "type": "string"
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "magnet"
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
       "avid": {
        "type": "string"
       },
       "state": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "note": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "archived_paths": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "next_action_at": {
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
       "release_date": {
        "anyOf": [
         {
          "type": "string",
          "format": "date"
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
       "replacing": {
        "items": {
         "type": "string"
        },
        "type": "array",
        "default": []
       }
      },
      "type": "object",
      "required": [
       "avid",
       "state",
       "source",
       "note",
       "archived_paths",
       "next_action_at",
       "release_date",
       "created_at",
       "updated_at"
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
        "/acquisitions/{avid}/ignore",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "avid",
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
       "avid": {
        "type": "string"
       },
       "state": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "note": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "archived_paths": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "next_action_at": {
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
       "release_date": {
        "anyOf": [
         {
          "type": "string",
          "format": "date"
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
       "replacing": {
        "items": {
         "type": "string"
        },
        "type": "array",
        "default": []
       }
      },
      "type": "object",
      "required": [
       "avid",
       "state",
       "source",
       "note",
       "archived_paths",
       "next_action_at",
       "release_date",
       "created_at",
       "updated_at"
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
        "/acquisitions/{avid}/resume",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "avid",
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
       "avid": {
        "type": "string"
       },
       "state": {
        "type": "string"
       },
       "source": {
        "type": "string"
       },
       "note": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "archived_paths": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "next_action_at": {
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
       "release_date": {
        "anyOf": [
         {
          "type": "string",
          "format": "date"
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
       "replacing": {
        "items": {
         "type": "string"
        },
        "type": "array",
        "default": []
       }
      },
      "type": "object",
      "required": [
       "avid",
       "state",
       "source",
       "note",
       "archived_paths",
       "next_action_at",
       "release_date",
       "created_at",
       "updated_at"
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
        "/manual/directories",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "path",
   "in": "query",
   "required": false,
   "schema": {
    "type": "string",
    "default": "/"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "path": {
        "type": "string"
       },
       "parent": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "entries": {
        "items": {
         "properties": {
          "path": {
           "type": "string"
          },
          "name": {
           "type": "string"
          },
          "configured": {
           "type": "boolean"
          },
          "routed": {
           "type": "boolean"
          }
         },
         "type": "object",
         "required": [
          "path",
          "name",
          "configured",
          "routed"
         ]
        },
        "type": "array"
       },
       "default_path": {
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
       "path",
       "parent",
       "entries",
       "default_path"
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
        "/manual",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "inputs": {
       "items": {
        "type": "string"
       },
       "type": "array"
      },
      "task_dir_path": {
       "type": "string"
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "inputs",
      "task_dir_path"
     ]
    }
   }
  },
  "required": true
 },
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "task_dir_path": {
        "type": "string"
       },
       "items": {
        "items": {
         "properties": {
          "text": {
           "type": "string"
          },
          "avid": {
           "anyOf": [
            {
             "type": "string"
            },
            {
             "type": "null"
            }
           ]
          },
          "outcome": {
           "type": "string"
          },
          "archived_paths": {
           "items": {
            "type": "string"
           },
           "type": "array"
          }
         },
         "type": "object",
         "required": [
          "text",
          "avid",
          "outcome",
          "archived_paths"
         ]
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "task_dir_path",
       "items"
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
        "GET",
        "/replace",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "avid",
   "in": "query",
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
       "avid": {
        "type": "string"
       },
       "holdings": {
        "items": {
         "type": "string"
        },
        "type": "array"
       },
       "state": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "task_dir_path": {
        "anyOf": [
         {
          "type": "string"
         },
         {
          "type": "null"
         }
        ]
       },
       "blocked": {
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
       "avid",
       "holdings",
       "state",
       "task_dir_path",
       "blocked"
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
        "/replace",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "requestBody": {
  "required": true,
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "avid": {
       "type": "string"
      },
      "magnet": {
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
     "additionalProperties": false,
     "type": "object",
     "required": [
      "avid"
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
       "plan": {
        "properties": {
         "avid": {
          "type": "string"
         },
         "holdings": {
          "items": {
           "type": "string"
          },
          "type": "array"
         },
         "state": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "task_dir_path": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "blocked": {
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
         "avid",
         "holdings",
         "state",
         "task_dir_path",
         "blocked"
        ]
       },
       "outcome": {
        "type": "string"
       },
       "acquisition": {
        "properties": {
         "avid": {
          "type": "string"
         },
         "state": {
          "type": "string"
         },
         "source": {
          "type": "string"
         },
         "note": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "archived_paths": {
          "items": {
           "type": "string"
          },
          "type": "array"
         },
         "next_action_at": {
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
         "release_date": {
          "anyOf": [
           {
            "type": "string",
            "format": "date"
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
         "replacing": {
          "items": {
           "type": "string"
          },
          "type": "array",
          "default": []
         }
        },
        "type": "object",
        "required": [
         "avid",
         "state",
         "source",
         "note",
         "archived_paths",
         "next_action_at",
         "release_date",
         "created_at",
         "updated_at"
        ]
       }
      },
      "type": "object",
      "required": [
       "plan",
       "outcome",
       "acquisition"
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
        "/tracker",
        r#"{
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "running": {
        "type": "boolean"
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
       },
       "last_polled_at": {
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
       },
       "last_stats": {
        "additionalProperties": {
         "type": "integer"
        },
        "type": "object"
       }
      },
      "type": "object",
      "required": [
       "running",
       "reason",
       "last_polled_at",
       "last_error",
       "last_stats"
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

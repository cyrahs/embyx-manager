//! The OpenAPI operation objects of the fill-actor routes, as the baseline's document has
//! them (`goldens/api/openapi_main.json`) with `$ref`s inlined, the documentation-only keys
//! left out and FastAPI's 422 response added by [`crate::openapi::operation`]. Generated
//! from the golden; the structural test compares the two documents.

/// Method, path below the router's prefix, and operation object of each route.
const OPERATIONS: &[(&str, &str, &str)] = &[
    (
        "POST",
        "/avid-actors",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "avid": {
       "type": "string",
       "maxLength": 128,
       "minLength": 1
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
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "avid": {
        "type": "string"
       },
       "actors": {
        "items": {
         "properties": {
          "actor_id": {
           "type": "string"
          },
          "name": {
           "type": "string"
          }
         },
         "type": "object",
         "required": [
          "actor_id",
          "name"
         ]
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "avid",
       "actors"
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
        "/plans",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "actor_ids": {
       "items": {
        "type": "string"
       },
       "type": "array",
       "minItems": 1
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "actor_ids"
     ]
    }
   }
  },
  "required": true
 },
 "responses": {
  "202": {
   "content": {
    "application/json": {
     "schema": {}
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
        "/plans/{plan_id}",
        r#"{
 "parameters": [
  {
   "name": "plan_id",
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
       "job": {
        "properties": {
         "job_id": {
          "type": "string"
         },
         "plan_id": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "operation": {
          "type": "string"
         },
         "state": {
          "type": "string"
         },
         "created_at": {
          "type": "string",
          "format": "date-time"
         },
         "updated_at": {
          "type": "string",
          "format": "date-time"
         },
         "error_code": {
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
          "properties": {
           "stage": {
            "type": "string"
           },
           "completed": {
            "type": "integer"
           },
           "total": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "unit": {
            "type": "string"
           },
           "current": {
            "anyOf": [
             {
              "type": "string"
             },
             {
              "type": "null"
             }
            ]
           },
           "stage_started_at": {
            "type": "string",
            "format": "date-time"
           },
           "updated_at": {
            "type": "string",
            "format": "date-time"
           },
           "percent": {
            "anyOf": [
             {
              "type": "number"
             },
             {
              "type": "null"
             }
            ]
           },
           "eta_seconds": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "elapsed_seconds": {
            "type": "integer"
           },
           "last_progress_seconds": {
            "type": "integer"
           }
          },
          "type": "object",
          "required": [
           "stage",
           "completed",
           "total",
           "unit",
           "current",
           "stage_started_at",
           "updated_at",
           "percent",
           "eta_seconds",
           "elapsed_seconds",
           "last_progress_seconds"
          ]
         }
        },
        "type": "object",
        "required": [
         "job_id",
         "plan_id",
         "operation",
         "state",
         "created_at",
         "updated_at",
         "error_code",
         "progress"
        ]
       },
       "plan": {
        "anyOf": [
         {
          "properties": {
           "plan_id": {
            "type": "string"
           },
           "revision": {
            "type": "string"
           },
           "created_at": {
            "type": "string",
            "format": "date-time"
           },
           "expires_at": {
            "type": "string",
            "format": "date-time"
           },
           "actors": {
            "items": {
             "properties": {
              "actor_id": {
               "type": "string"
              },
              "scraped_count": {
               "type": "integer",
               "minimum": 0.0
              },
              "video_ids": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              },
              "error_code": {
               "anyOf": [
                {
                 "type": "string"
                },
                {
                 "type": "null"
                }
               ]
              },
              "actor_name": {
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
              "error_message": {
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
              "actor_id",
              "scraped_count"
             ]
            },
            "type": "array"
           },
           "videos": {
            "items": {
             "properties": {
              "video_id": {
               "type": "string"
              },
              "actor_ids": {
               "items": {
                "type": "string"
               },
               "type": "array"
              },
              "state": {
               "type": "string",
               "enum": [
                "exists",
                "additional_found",
                "magnet_found",
                "queued",
                "submitted",
                "already_tracked",
                "submit_failed",
                "missing",
                "invalid_video_id",
                "scan_failed"
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
              "existing_files": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              },
              "move_candidates": {
               "items": {
                "properties": {
                 "candidate_id": {
                  "type": "string"
                 },
                 "video_id": {
                  "type": "string"
                 },
                 "file_name": {
                  "type": "string"
                 },
                 "source_label": {
                  "type": "string"
                 },
                 "destination_conflict": {
                  "type": "boolean"
                 }
                },
                "type": "object",
                "required": [
                 "candidate_id",
                 "video_id",
                 "file_name",
                 "source_label",
                 "destination_conflict"
                ]
               },
               "type": "array",
               "default": []
              },
              "warnings": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              }
             },
             "type": "object",
             "required": [
              "video_id",
              "actor_ids",
              "state"
             ]
            },
            "type": "array"
           }
          },
          "type": "object",
          "required": [
           "plan_id",
           "revision",
           "created_at",
           "expires_at",
           "actors",
           "videos"
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
       "job",
       "plan"
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
        "/plans/{plan_id}/cancel",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "plan_id",
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
       "job": {
        "properties": {
         "job_id": {
          "type": "string"
         },
         "plan_id": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "operation": {
          "type": "string"
         },
         "state": {
          "type": "string"
         },
         "created_at": {
          "type": "string",
          "format": "date-time"
         },
         "updated_at": {
          "type": "string",
          "format": "date-time"
         },
         "error_code": {
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
          "properties": {
           "stage": {
            "type": "string"
           },
           "completed": {
            "type": "integer"
           },
           "total": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "unit": {
            "type": "string"
           },
           "current": {
            "anyOf": [
             {
              "type": "string"
             },
             {
              "type": "null"
             }
            ]
           },
           "stage_started_at": {
            "type": "string",
            "format": "date-time"
           },
           "updated_at": {
            "type": "string",
            "format": "date-time"
           },
           "percent": {
            "anyOf": [
             {
              "type": "number"
             },
             {
              "type": "null"
             }
            ]
           },
           "eta_seconds": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "elapsed_seconds": {
            "type": "integer"
           },
           "last_progress_seconds": {
            "type": "integer"
           }
          },
          "type": "object",
          "required": [
           "stage",
           "completed",
           "total",
           "unit",
           "current",
           "stage_started_at",
           "updated_at",
           "percent",
           "eta_seconds",
           "elapsed_seconds",
           "last_progress_seconds"
          ]
         }
        },
        "type": "object",
        "required": [
         "job_id",
         "plan_id",
         "operation",
         "state",
         "created_at",
         "updated_at",
         "error_code",
         "progress"
        ]
       },
       "plan": {
        "anyOf": [
         {
          "properties": {
           "plan_id": {
            "type": "string"
           },
           "revision": {
            "type": "string"
           },
           "created_at": {
            "type": "string",
            "format": "date-time"
           },
           "expires_at": {
            "type": "string",
            "format": "date-time"
           },
           "actors": {
            "items": {
             "properties": {
              "actor_id": {
               "type": "string"
              },
              "scraped_count": {
               "type": "integer",
               "minimum": 0.0
              },
              "video_ids": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              },
              "error_code": {
               "anyOf": [
                {
                 "type": "string"
                },
                {
                 "type": "null"
                }
               ]
              },
              "actor_name": {
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
              "error_message": {
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
              "actor_id",
              "scraped_count"
             ]
            },
            "type": "array"
           },
           "videos": {
            "items": {
             "properties": {
              "video_id": {
               "type": "string"
              },
              "actor_ids": {
               "items": {
                "type": "string"
               },
               "type": "array"
              },
              "state": {
               "type": "string",
               "enum": [
                "exists",
                "additional_found",
                "magnet_found",
                "queued",
                "submitted",
                "already_tracked",
                "submit_failed",
                "missing",
                "invalid_video_id",
                "scan_failed"
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
              "existing_files": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              },
              "move_candidates": {
               "items": {
                "properties": {
                 "candidate_id": {
                  "type": "string"
                 },
                 "video_id": {
                  "type": "string"
                 },
                 "file_name": {
                  "type": "string"
                 },
                 "source_label": {
                  "type": "string"
                 },
                 "destination_conflict": {
                  "type": "boolean"
                 }
                },
                "type": "object",
                "required": [
                 "candidate_id",
                 "video_id",
                 "file_name",
                 "source_label",
                 "destination_conflict"
                ]
               },
               "type": "array",
               "default": []
              },
              "warnings": {
               "items": {
                "type": "string"
               },
               "type": "array",
               "default": []
              }
             },
             "type": "object",
             "required": [
              "video_id",
              "actor_ids",
              "state"
             ]
            },
            "type": "array"
           }
          },
          "type": "object",
          "required": [
           "plan_id",
           "revision",
           "created_at",
           "expires_at",
           "actors",
           "videos"
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
       "job",
       "plan"
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
        "/plans/{plan_id}/apply-jobs",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "plan_id",
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
      "revision": {
       "type": "string",
       "maxLength": 256,
       "minLength": 1
      },
      "candidate_ids": {
       "items": {
        "type": "string"
       },
       "type": "array",
       "maxItems": 5000
      },
      "request_id": {
       "type": "string",
       "maxLength": 128,
       "minLength": 16,
       "pattern": "^[A-Za-z0-9_-]+$"
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "revision",
      "request_id"
     ]
    }
   }
  }
 },
 "responses": {
  "202": {
   "content": {
    "application/json": {
     "schema": {
      "properties": {
       "job": {
        "properties": {
         "job_id": {
          "type": "string"
         },
         "plan_id": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "operation": {
          "type": "string"
         },
         "state": {
          "type": "string"
         },
         "created_at": {
          "type": "string",
          "format": "date-time"
         },
         "updated_at": {
          "type": "string",
          "format": "date-time"
         },
         "error_code": {
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
          "properties": {
           "stage": {
            "type": "string"
           },
           "completed": {
            "type": "integer"
           },
           "total": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "unit": {
            "type": "string"
           },
           "current": {
            "anyOf": [
             {
              "type": "string"
             },
             {
              "type": "null"
             }
            ]
           },
           "stage_started_at": {
            "type": "string",
            "format": "date-time"
           },
           "updated_at": {
            "type": "string",
            "format": "date-time"
           },
           "percent": {
            "anyOf": [
             {
              "type": "number"
             },
             {
              "type": "null"
             }
            ]
           },
           "eta_seconds": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "elapsed_seconds": {
            "type": "integer"
           },
           "last_progress_seconds": {
            "type": "integer"
           }
          },
          "type": "object",
          "required": [
           "stage",
           "completed",
           "total",
           "unit",
           "current",
           "stage_started_at",
           "updated_at",
           "percent",
           "eta_seconds",
           "elapsed_seconds",
           "last_progress_seconds"
          ]
         }
        },
        "type": "object",
        "required": [
         "job_id",
         "plan_id",
         "operation",
         "state",
         "created_at",
         "updated_at",
         "error_code",
         "progress"
        ]
       },
       "result": {
        "anyOf": [
         {
          "properties": {
           "plan_id": {
            "type": "string"
           },
           "revision": {
            "type": "string"
           },
           "state": {
            "type": "string",
            "enum": [
             "succeeded",
             "partial_failed",
             "failed"
            ]
           },
           "results": {
            "items": {
             "properties": {
              "candidate_id": {
               "type": "string"
              },
              "video_id": {
               "type": "string"
              },
              "file_name": {
               "type": "string"
              },
              "state": {
               "type": "string",
               "enum": [
                "moved",
                "stale",
                "conflict",
                "invalid_path",
                "failed"
               ]
              },
              "error_code": {
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
              "candidate_id",
              "video_id",
              "file_name",
              "state"
             ]
            },
            "type": "array"
           }
          },
          "type": "object",
          "required": [
           "plan_id",
           "revision",
           "state",
           "results"
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
       "job",
       "result"
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
        "/apply-jobs/{job_id}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "job_id",
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
       "job": {
        "properties": {
         "job_id": {
          "type": "string"
         },
         "plan_id": {
          "anyOf": [
           {
            "type": "string"
           },
           {
            "type": "null"
           }
          ]
         },
         "operation": {
          "type": "string"
         },
         "state": {
          "type": "string"
         },
         "created_at": {
          "type": "string",
          "format": "date-time"
         },
         "updated_at": {
          "type": "string",
          "format": "date-time"
         },
         "error_code": {
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
          "properties": {
           "stage": {
            "type": "string"
           },
           "completed": {
            "type": "integer"
           },
           "total": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "unit": {
            "type": "string"
           },
           "current": {
            "anyOf": [
             {
              "type": "string"
             },
             {
              "type": "null"
             }
            ]
           },
           "stage_started_at": {
            "type": "string",
            "format": "date-time"
           },
           "updated_at": {
            "type": "string",
            "format": "date-time"
           },
           "percent": {
            "anyOf": [
             {
              "type": "number"
             },
             {
              "type": "null"
             }
            ]
           },
           "eta_seconds": {
            "anyOf": [
             {
              "type": "integer"
             },
             {
              "type": "null"
             }
            ]
           },
           "elapsed_seconds": {
            "type": "integer"
           },
           "last_progress_seconds": {
            "type": "integer"
           }
          },
          "type": "object",
          "required": [
           "stage",
           "completed",
           "total",
           "unit",
           "current",
           "stage_started_at",
           "updated_at",
           "percent",
           "eta_seconds",
           "elapsed_seconds",
           "last_progress_seconds"
          ]
         }
        },
        "type": "object",
        "required": [
         "job_id",
         "plan_id",
         "operation",
         "state",
         "created_at",
         "updated_at",
         "error_code",
         "progress"
        ]
       },
       "result": {
        "anyOf": [
         {
          "properties": {
           "plan_id": {
            "type": "string"
           },
           "revision": {
            "type": "string"
           },
           "state": {
            "type": "string",
            "enum": [
             "succeeded",
             "partial_failed",
             "failed"
            ]
           },
           "results": {
            "items": {
             "properties": {
              "candidate_id": {
               "type": "string"
              },
              "video_id": {
               "type": "string"
              },
              "file_name": {
               "type": "string"
              },
              "state": {
               "type": "string",
               "enum": [
                "moved",
                "stale",
                "conflict",
                "invalid_path",
                "failed"
               ]
              },
              "error_code": {
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
              "candidate_id",
              "video_id",
              "file_name",
              "state"
             ]
            },
            "type": "array"
           }
          },
          "type": "object",
          "required": [
           "plan_id",
           "revision",
           "state",
           "results"
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
       "job",
       "result"
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
        "/plans/{plan_id}/apply",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "plan_id",
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
      "revision": {
       "type": "string",
       "maxLength": 256,
       "minLength": 1
      },
      "candidate_ids": {
       "items": {
        "type": "string"
       },
       "type": "array",
       "maxItems": 5000
      }
     },
     "additionalProperties": false,
     "type": "object",
     "required": [
      "revision"
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
       "plan_id": {
        "type": "string"
       },
       "revision": {
        "type": "string"
       },
       "state": {
        "type": "string",
        "enum": [
         "succeeded",
         "partial_failed",
         "failed"
        ]
       },
       "results": {
        "items": {
         "properties": {
          "candidate_id": {
           "type": "string"
          },
          "video_id": {
           "type": "string"
          },
          "file_name": {
           "type": "string"
          },
          "state": {
           "type": "string",
           "enum": [
            "moved",
            "stale",
            "conflict",
            "invalid_path",
            "failed"
           ]
          },
          "error_code": {
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
          "candidate_id",
          "video_id",
          "file_name",
          "state"
         ]
        },
        "type": "array"
       }
      },
      "type": "object",
      "required": [
       "plan_id",
       "revision",
       "state",
       "results"
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

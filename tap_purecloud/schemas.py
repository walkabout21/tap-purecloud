historical_adherence = {
    'type': 'object',
    'properties': {
        "userId": {
            "type": ["null", "string"]
        },
        "startDate": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "endDate": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "impact": {
            "type": ["null", "string"]
        },
        "exceptionInfo": {
            "type": ["null", "array"],
            "items": {
                "type": ["object", "null"]
            }
        },
        "management_unit_id": {
            "type": ["null", "string"]
        },
        "dayMetrics": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {
                    "dayStartOffsetSecs": {
                        "type": ["null", "number"]
                    },
                    "adherenceScheduleSecs": {
                        "type": ["null", "number"]
                    },
                    "conformanceScheduleSecs": {
                        "type": ["null", "number"]
                    },
                    "conformanceActualSecs": {
                        "type": ["null", "number"]
                    },
                    "exceptionCount": {
                        "type": ["null", "number"]
                    },
                    "exceptionDurationSecs": {
                        "type": ["null", "number"]
                    },
                    "impactSeconds": {
                        "type": ["null", "number"]
                    },
                    "scheduleLengthSecs": {
                        "type": ["null", "number"]
                    },
                    "actualLengthSecs": {
                        "type": ["null", "number"]
                    }
                }
            }
        }
    }
}

user = {
    'type': 'object',
    'properties': {
        'email': {
            'type': 'string',
            'description': 'email for the user',
        },
        'id': {
            'type': 'string',
            'description': 'id for the user',
        },
        'name': {
            'type': 'string',
            'description': 'name for the user',
        },
        'username': {
            'type': 'string',
            'description': 'username for the user',
        }
    }
}

group = {
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string',
            'description': 'name for the group',
        },
        'id': {
            'type': 'string',
            'description': 'id for the group',
        },
        'state': {
            'type': 'string',
            'description': 'state for the group',
        },
        'visibility': {
            'type': 'string',
            'description': 'visibility for the group',
        }
    }
}

location = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'description': 'id for the location',
        },
        'name': {
            'type': 'string',
            'description': 'name for the location',
        },
        'state': {
            'type': 'string',
            'description': 'state for the location',
        }
    }
}


conversation = {
    'type': 'object',
    'properties': {
        'conversation_id': {
            'type': 'string',
            'description': 'id for the conversation',
        },
        'conversation_start': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'start timestamp for the conversation',
        },
        'conversation_end': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'end timestamp for the conversation',
        },
        "division_ids": {
            "type": ["array", "null"],
            "items": {
                "type": "string"
            }
        },
        "conversation_initiator": {
            "type": ["string", "null"]
        },
        "media_stats_min_conversation_mos": {
            "type": ["number", "null"]
        },
        "media_stats_min_conversation_r_factor": {
            "type": ["number", "null"]
        },
        "originating_direction": {
            "type": ["string", "null"]
        },
        "customer_participation": {
            "type": ["boolean", "null"]
        },
    }
}

conversation_participant = {
   'type': 'object', 
   'properties': {
        'conversation_id': {
            'type': 'string',
            'description': 'id for the conversation'
        },
        'participant_id': {
            'type': 'string',
            'description': 'id for the participant'
        },
        'participant_name': {
            'type': ['string', 'null'],
            'description': 'name for the participant'
        },
        "user_id": {
            "type": ["string", "null"]
        },
        "flagged_reason": {
            "type": ["string", "null"]
        },
        "purpose": {
            "type": ["string", "null"]
        },
   }
}

conversation_participant_session = {
    'type': 'object', 
    'properties': {
        'conversation_id': {
            'type': 'string',
            'description': 'id for the conversation'
        },
        'participant_id': {
            'type': 'string',
            'description': 'id for the participant'
        },
        'session_id': {
            'type': 'string',
            'description': 'id for the session'
        },
        'direction': {
            'type': 'string'
        },
        "acw_skipped": {
            "type": ["boolean", "null"]
        },
        "assigner_id": {
            "type": ["string", "null"]
        },
        "media_type": {
            "type": ["string", "null"]
        },
        "message_type": {
            "type": ["string", "null"]
        },
        "outbound_campaign_id": {
            "type": ["string", "null"]
        },
        "provider": {
            "type": ["string", "null"]
        },
        "recording": {
            "type": ["boolean", "null"]
        },
        "selected_agent_id": {
            "type": ["string", "null"]
        }
    }
}

conversation_participant_session_segment = {
    "type": "object",
    "properties": {
        "conversation_id": {
            "type": "string",
            "description": "id for the conversation"
        },
        "destination_conversation_id": {
            "type": "string"
        },
        "participant_id": {
            "type": "string",
            "description": "id for the participant"
        },
        "session_id": {
            "type": "string",
            "description": "id for the session"
        },
        "destination_session_id": {
            "type": "string"
        },
        "segment_start": {
            "type": "string",
            "format": "date-time",
            "description": "start datetime for the segment"
        },
        "segment_end": {
            "type": ["string", "null"],
            "format": "date-time",
            "description": "end datetime for the segment"
        },
        "segment_type": {
            "type": "string"
        },
        "queue_id": {
            "type": ["string", "null"]
        },
        "group_id": {
            "type": ["string", "null"]
        },
        "subject": {
            "type": ["string", "null"]
        },
        "source_conversation_id": {
            "type": ["string", "null"]
        },
        "source_session_id": {
            "type": ["string", "null"]
        }
    }
}

conversation_participant_session_metric = {
    "type": "object",
    "properties": {
        "conversation_id": {
            "type": "string",
            "description": "id for the conversation"
        },
        "participant_id": {
            "type": "string",
            "description": "id for the participant"
        },
        "session_id": {
            "type": "string",
            "description": "id for the session"
        },
        "name": {
            "type": "string"
        },
        "value": {
            "type": "number"
        },
        "emit_date": {
            "type": "string",
            "format": "date-time"
        }
    }
}


user_state = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'description': 'id for the user state',
        },
        'user_id': {
            'type': 'string',
            'description': 'id for the user',
        },
        'start_time': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'start time',
        },
        'end_time': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'end time',
        },
        'state': {
            'type': 'string',
            'description': 'state'
        },
        'state_id': {
            'type': ['string', 'null'],
            'description': 'state id'
        },
        'type': {
            'type': 'string',
            'description': 'message type'
        }
    }
}

management_unit = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'description': 'id for the management unit',
        },
        'name': {
            'type': 'string',
            'description': 'name for the management unit',
        }
    }
}

activity_code = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'description': 'id for the activity code',
        },
        'management_unit_id': {
            'type': 'string',
            'description': 'id for the management unit for this activity code',
        },
        'name': {
            'type': 'string',
            'description': 'name for this activity code',
        },
        'category': {
            'type': 'string',
            'description': 'category for this activity code',
        }
    }
}

management_unit_users = {
    'type': 'object',
    'properties': {
        'management_unit_id': {
            'type': 'string',
            'description': 'id for the management unit for this user',
        },
        'user_id': {
            'type': 'string',
            'description': 'id for the user',
        }
    }
}

user_schedule = {
    'type': 'object',
    'properties': {
        'user_id': {
            'type': 'string',
            'description': 'id for the user',
        },
        'start_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'date for the sync',
        },
        "metadata": {
            "type": ["object", "null"],
            "properties": {
                "version": {
                    "type": "number"
                },
                "modified_by": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string"
                        },
                    }
                },
                "date_modified": {
                    "type": "string",
                    "format": "date-time"
                },
                "created_by": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string"
                        },
                    }
                },
                "date_created": {
                    "type": "string",
                    "format": "date-time"
                }
            }
        },
        "work_plan_id": {
            "type": ["string", "null"]
        }
    }
}

user_schedule_shift = {
    'type': 'object',
    'properties': {
        "id": {
            "type": "string"
        },
        "user_id": {
            "type": "string"
        },
        "length_in_minutes": {
            "type": "number"
        },
        "week_schedule": {
            "type": "object",
            "properties": {
                "id": {
                    "type": "string"
                },
                "week_date": {
                    "type": "string"
                }
            }
        },
        'start_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'date for the shift',
        },
        "delete": {
            "type": ["boolean", "null"]
        },
        "manually_edited": {
            "type": ["boolean", "null"]
        }
    }
}

user_schedule_shift_activity = {
    'type': 'object',
    'properties': {
        "user_id": {
            "type": "string"
        },
        "shift_id": {
            "type": "string"
        },
        'activity_code_id': {
            'type': 'string',
            'description': 'id for the activity_code',
        },
        "start_date": {
            "type": "string",
            "format": "date-time",
        },
        "length_in_minutes": {
            "type": "number"
        },
        "description": {
            "type": "string"
        },
        "counts_as_paid_time": {
            "type": "boolean"
        },
        "is_dst_fallback": {
            "type": ["boolean", "null"]
        },
        "time_off_request_id": {
            "type": ["string", "null"]
        }

    }
}

presence_label = {
    'type': 'object',
    'properties': {
        'en_US': {
            'type': 'string',
            'description': 'English presence label'
        }
    }
}

presence = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'description': 'presence id',
        },
        'language_labels': presence_label,
        'created_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'presence creation date',
        },
        'modified_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'presence modification date',
        }
    }
}

queue = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'name': 'queue id',
        },
        'name': {
            'type': 'string',
            'name': 'queue name',
        },
        'member_count': {
            'type': 'number',
            'name': 'queue member count',
        },
        'created_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'queue creation date',
        },
        'modified_date': {
            'type': ['string', 'null'],
            'format': 'date-time',
            'description': 'queue modification date',
        }
    }
}

queue_membership = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'name': 'id for the user membership in this queue',
        },
        'queue_id': {
            'type': 'string',
            'name': 'id for the queue',
        },
        'user_id': {
            'type': 'string',
            'name': 'user id for this queue',
        }
    }
}

queue_wrapup = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'string',
            'name': 'id for the wrapup code in this queue',
        },
    }
}

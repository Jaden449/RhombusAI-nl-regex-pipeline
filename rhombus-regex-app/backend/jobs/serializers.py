from rest_framework import serializers

from .models import Job


class JobCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = [
            "input_file",
            "nl_prompt",
            "target_column",
            "replacement_value",
        ]

    def validate_input_file(self, value):
        allowed_ext = (".csv", ".xlsx", ".xls")
        if not value.name.lower().endswith(allowed_ext):
            raise serializers.ValidationError("Only .csv, .xlsx, or .xls files are supported.")
        return value

    def validate_nl_prompt(self, value):
        if not value or not value.strip():
            raise serializers.ValidationError("Describe the pattern you want to find, e.g. 'find email addresses'.")
        return value.strip()


class JobStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = [
            "id",
            "status",
            "progress",
            "original_filename",
            "nl_prompt",
            "target_column",
            "replacement_value",
            "regex_pattern",
            "regex_source",
            "error_message",
            "row_count",
            "matched_count",
            "created_at",
            "updated_at",
        ]

# Data Analysis Request

Please analyze the following data and provide insights:

## Dataset

```json
{{event.data}}
```

## Analysis Requirements

- Identify key trends and patterns
- Calculate relevant statistics
- Compare against historical baselines if available
- Flag any anomalies or outliers
- Provide business recommendations

## Focus Areas

{{#if event.data.focus_areas}}
Prioritize analysis of: {{event.data.focus_areas}}
{{else}}
Perform a comprehensive analysis across all metrics.
{{/if}}

## Target Audience

{{#if event.data.audience}}
Tailor the analysis for: {{event.data.audience}}
{{else}}
Assume a general business stakeholder audience.
{{/if}}

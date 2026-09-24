/**
 * Chart Explanations Utility
 * Provides contextual information tooltips for report charts using SweetAlert2
 */

const ChartExplanations = {
  // Chart explanation database
  explanations: {
    // Report 22 — Operations
    'tree_chart': {
      title: 'Modality → AE Source → Procedures',
      purpose: 'Shows the hierarchical breakdown of study volume by imaging modality, then by DICOM AE title (device source), and finally the top 5 procedures per AE.',
      valueMeaning: 'Node size represents the number of studies. Larger nodes indicate higher study counts.',
      interpretation: 'Use this to identify which AE titles (devices) handle the most studies per modality, and which procedures are most common. Useful for load balancing and resource allocation.'
    },
    'c2': {
      title: 'Case Status Distribution',
      purpose: 'Displays the count of studies grouped by their current DICOM study status (e.g., Read, Unread, Dictated).',
      valueMeaning: 'Each bar represents a status category. The height of the bar shows how many studies are in that status.',
      interpretation: 'Higher "Read" counts indicate good workflow completion. High "Unread" or "Cancelled" counts may indicate backlogs or issues. Click any bar to drill down into individual studies.'
    },
    'c3': {
      title: 'Top Referring Physicians (Study Volume)',
      purpose: 'Ranks the 10 physicians who referred the highest total number of studies in the selected period.',
      valueMeaning: 'Each bar shows the total number of studies referred by that physician.',
      interpretation: 'Higher values indicate more active physicians. A single physician may refer the same patient multiple times — this chart counts studies, not patients.'
    },
    'c_unique_phys': {
      title: 'Physician Loyalty (Unique Patient Count)',
      purpose: 'Ranks the 10 physicians with the broadest patient reach — by the number of distinct patients they referred.',
      valueMeaning: 'Each bar shows the number of unique patients referred by that physician.',
      interpretation: 'High unique patient count relative to total study volume indicates a physician refers many different patients (broader patient base). Lower ratio suggests repeat patient visits from the same physician.'
    },
    'c_phys_mod': {
      title: 'Physician → Modality Preference (Top 10)',
      purpose: 'Shows which modality each top referring physician sends most to. Uses stacked bars, coloured by modality.',
      valueMeaning: 'Each colored segment represents a modality. The width of the segment shows the proportion of studies in that modality.',
      interpretation: 'Use to understand physician specialisation — some doctors may favor CT for abdominal cases, while others prefer US. Helps predict future demand and resource needs per physician group.'
    },
    'c_phys_age': {
      title: 'Average Patient Age per Physician',
      purpose: 'Shows the mean age of patients referred by each physician (top 15 by volume, min 5 studies).',
      valueMeaning: 'Each bar shows the average age of patients referred by that physician.',
      interpretation: 'Helps identify physician specialisation profile. Pediatric specialists will show lower averages; geriatric specialists higher. Higher variance indicates broader patient demographics.'
    },
    'c_proc_age': {
      title: 'Age Distribution per Procedure Code',
      purpose: 'Displays the age spread for the top 20 procedure codes by volume. Whiskers = min/max, box = IQR (Q1–Q3), line = median, ◆ = mean.',
      valueMeaning: 'The box represents the middle 50% of patients (IQR). The line inside = median age. ◆ = average age. Whiskers show the full range.',
      interpretation: 'Use to understand typical patient demographics per procedure. A wide distribution suggests the procedure is used across age groups. A narrow distribution suggests age-specific procedures (e.g., pediatric screening).'
    },
    'c_phys_status': {
      title: 'Study Status per Top Physician',
      purpose: 'Shows the status breakdown (Completed, Cancelled, Unread, etc.) for the top 10 referring physicians by volume.',
      valueMeaning: 'Stacked bars show the proportion of studies in each status for each physician.',
      interpretation: 'High cancellation or incomplete rates for a physician may indicate ordering behaviour issues or patient factors. Compare physicians to identify outliers.'
    },
    'c_gender': {
      title: 'Gender (Rose)',
      purpose: 'Shows the proportion of studies by patient gender (M / F) in the selected period.',
      valueMeaning: 'The rose (petal) size and area represent the proportion of studies for each gender.',
      interpretation: 'Indicates patient demographics. A balanced 50/50 split suggests typical demographics. Skewed ratios may indicate specialized services or referral patterns.'
    },
    'c_age': {
      title: 'Age Group (Wave)',
      purpose: 'Shows the distribution of studies across patient age groups (0–110 years).',
      valueMeaning: 'The wave height at each age represents the number of studies for patients in that age.',
      interpretation: 'Smooth, bell-shaped curve suggests typical population distribution. Spikes indicate age-specific services or pathology screening programs.'
    },

    // Report 25 — Executive
    'operations-volume': {
      title: 'Total Studies',
      purpose: 'Shows the total number of imaging studies completed in the selected period.',
      valueMeaning: 'A single count of all completed studies.',
      interpretation: 'Higher values indicate busier periods. Use to track workload trends and resource planning.'
    },
    'operations-scan-time': {
      title: 'Active Scan Time',
      purpose: 'Shows the total hours machines were actively scanning, calculated from procedure durations.',
      valueMeaning: 'Total productive machine hours (not including downtime or idle time).',
      interpretation: 'Compare to available operating hours to calculate utilization rate. Lower values may indicate underutilization or bottlenecks.'
    },
    'operations-stress': {
      title: 'High Stress Devices',
      purpose: 'Shows the count of AE titles (devices) operating at >85% utilization.',
      valueMeaning: 'Each device counted represents critical utilization above safety thresholds.',
      interpretation: 'High stress devices are at risk of failures or require maintenance. Consider load balancing or expanding capacity.'
    },
    'peak-hourly': {
      title: 'Peak Hours — Hourly Arrival Distribution',
      purpose: 'Shows when studies arrive throughout the day. Identifies peak workload periods.',
      valueMeaning: 'Each bar shows the number of studies arriving in that hour.',
      interpretation: 'Use to staff appropriately and predict wait times. Plan maintenance during off-peak hours.'
    },
    'infra-device-util': {
      title: 'Device Utilization by AE Title',
      purpose: 'Shows the current utilization rate (%) for each DICOM AE (device).',
      valueMeaning: 'Each bar represents a device, showing what % of available capacity is in use.',
      interpretation: 'Green = healthy (<70%), Yellow = caution (70–85%), Red = critical (>85%). Plan capacity expansion for red devices.'
    },
    'efficiency-tat': {
      title: 'TAT Across Modalities',
      purpose: 'Shows median turnaround time (order to final report) by modality.',
      valueMeaning: 'Each bar = hours from order to final report. Lower is better.',
      interpretation: 'Some modalities may be faster due to complexity. Outliers indicate workflow issues or staffing shortages.'
    },
    'tat-pacs-vs-ris': {
      title: 'TAT: PACS vs RIS Timestamps',
      purpose: 'Compares turnaround time using PACS timestamps vs RIS timestamps. Helps identify data source reliability.',
      valueMeaning: 'Two bars per modality: blue = PACS-based TAT, orange = RIS-based TAT.',
      interpretation: 'Large differences suggest one source may have timestamp issues. RIS timing is generally more accurate for clinical workflows.'
    },

    // ORU Analytics
    'nlp-normal-abnormal': {
      title: 'Normal vs Abnormal Rate',
      purpose: 'Shows the proportion of ORU reports classified as Normal (no findings) vs Abnormal (has findings).',
      valueMeaning: 'Pie chart showing the percentage of each classification.',
      interpretation: 'Higher normal rate is typical for screening programs. Lower normal rate may indicate case selection bias or disease prevalence in the patient population.'
    },
    'nlp-critical-rate': {
      title: 'Critical Findings Rate',
      purpose: 'Shows the percentage of reports flagged as containing critical findings that need immediate notification.',
      valueMeaning: 'The count and percentage of critical findings relative to total reports.',
      interpretation: 'Baseline varies by institution (~3–10%). Spikes indicate potential quality issues or genuine disease increase. Compare to peer institutions for benchmarking.'
    },
    'nlp-word-cloud': {
      title: 'NLP Word Cloud',
      purpose: 'Visual representation of the most common finding keywords in reports. Larger text = more frequent keywords.',
      valueMeaning: 'Word size correlates with frequency across all analyzed reports.',
      interpretation: 'Identifies the most common pathologies and findings. Useful for understanding disease burden and case mix.'
    },
    'critical-findings-table': {
      title: 'Critical Findings Log',
      purpose: 'List of individual reports flagged as critical, with keywords, procedure code, and timestamps.',
      valueMeaning: 'Each row is a report with critical findings. Keywords are extracted by the NLP engine.',
      interpretation: 'Use for rapid triage and follow-up workflows. Archive these reports for audits and quality tracking.'
    },
    'nlp-procedure-breakdown': {
      title: 'Procedures with Abnormal Findings',
      purpose: 'Shows which procedure codes have the highest abnormal finding rates.',
      valueMeaning: 'Count of abnormal findings per procedure and percentage of that procedure type with findings.',
      interpretation: 'Procedure codes with high abnormal rates may indicate specific disease patterns or patient risk factors. Use for resource allocation.'
    }
  },

  /**
   * Show an explanation modal using SweetAlert2
   * @param {string} chartId - The chart ID key from explanations object
   */
  show: function(chartId) {
    const exp = this.explanations[chartId];
    if (!exp) {
      console.warn(`No explanation found for chart: ${chartId}`);
      return;
    }

    Swal.fire({
      title: exp.title,
      html: `
        <div style="text-align: left; font-size: 14px;">
          <div style="margin-bottom: 16px;">
            <p style="margin: 0 0 8px 0; font-weight: 600; color: #60a5fa;">Purpose</p>
            <p style="margin: 0; color: #cbd5e1;">${exp.purpose}</p>
          </div>
          <div style="margin-bottom: 16px;">
            <p style="margin: 0 0 8px 0; font-weight: 600; color: #60a5fa;">Value Meaning</p>
            <p style="margin: 0; color: #cbd5e1;">${exp.valueMeaning}</p>
          </div>
          <div>
            <p style="margin: 0 0 8px 0; font-weight: 600; color: #60a5fa;">Interpretation</p>
            <p style="margin: 0; color: #cbd5e1;">${exp.interpretation}</p>
          </div>
        </div>
      `,
      icon: 'info',
      confirmButtonText: 'Close',
      confirmButtonColor: '#60a5fa',
      background: '#232b28',
      color: '#e2e8f0',
      customClass: {
        container: 'swal-info-container',
        popup: 'swal-info-popup',
        title: 'swal-info-title',
        htmlContainer: 'swal-info-html',
        confirmButton: 'swal-info-button'
      }
    });
  }
};

// Add global styles for SweetAlert2 if not already present
function initChartExplanationStyles() {
  if (document.getElementById('chart-explanation-styles')) return;

  const styleEl = document.createElement('style');
  styleEl.id = 'chart-explanation-styles';
  styleEl.textContent = `
    .swal-info-container {
      --swal-modal-width: 520px !important;
    }
    .swal-info-popup {
      background: #232b28 !important;
      border: 1px solid #28332e !important;
      border-radius: 12px !important;
      box-shadow: 0 24px 60px rgba(0,0,0,0.6) !important;
    }
    .swal-info-title {
      color: #60a5fa !important;
      font-size: 18px !important;
      font-weight: 900 !important;
      margin-bottom: 16px !important;
    }
    .swal-info-html {
      color: #e2e8f0 !important;
      font-size: 13px !important;
    }
    .swal-info-button {
      background-color: #60a5fa !important;
      color: #0a1628 !important;
      font-weight: 800 !important;
      border-radius: 8px !important;
      padding: 10px 24px !important;
      text-transform: uppercase !important;
      letter-spacing: 0.05em !important;
      font-size: 11px !important;
    }
    .swal-info-button:hover {
      background-color: #3b82f6 !important;
      filter: brightness(1.1);
    }

    /* Info icon styling */
    .chart-info-icon {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 50%;
      background: rgba(96, 165, 250, 0.15);
      color: #60a5fa;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-left: 6px;
      flex-shrink: 0;
    }
    .chart-info-icon:hover {
      background: rgba(96, 165, 250, 0.3);
      transform: scale(1.1);
    }
    .chart-info-icon:active {
      transform: scale(0.95);
    }

    /* Light mode support */
    body.light .swal-info-popup {
      background: #ffffff !important;
      border-color: #e2e8f0 !important;
    }
    body.light .swal-info-title {
      color: #2b6cb0 !important;
    }
    body.light .swal-info-html {
      color: #1a202c !important;
    }
    body.light .swal-info-html p {
      color: #475569 !important;
    }
    body.light .swal-info-button {
      background-color: #0ea5e9 !important;
      color: #ffffff !important;
    }
    body.light .swal-info-button:hover {
      background-color: #0284c7 !important;
    }
    body.light .chart-info-icon {
      background: rgba(14, 165, 233, 0.15);
      color: #0284c7;
    }
    body.light .chart-info-icon:hover {
      background: rgba(14, 165, 233, 0.3);
    }
  `;
  document.head.appendChild(styleEl);
}

// Initialize styles when script loads
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initChartExplanationStyles);
} else {
  initChartExplanationStyles();
}

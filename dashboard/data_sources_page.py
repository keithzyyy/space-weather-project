"""Data-source attribution and limitations for the public dashboard."""

import streamlit as st


def render_data_sources_page() -> None:
    """Render source attribution for the frozen demonstration artifacts."""
    # Give the attribution page a stable, human-readable title.
    st.title("Data sources & attribution")

    # Explain the page's purpose before presenting source-specific details.
    st.caption(
        "Sources and limitations for the frozen artifacts shown in this "
        "dashboard."
    )

    # Make the dashboard's read-only boundary immediately visible.
    st.info(
        "This dashboard reads committed demonstration artifacts. It makes no "
        "live requests to either source and does not execute the data pipeline."
    )

    # Attribute the source interface used by the K-index pipeline.
    with st.container(border=True):
        # Name the provider and dataset family in plain language.
        st.subheader("Bureau of Meteorology K-index")

        # Link to the official interface documentation without asserting a licence.
        st.markdown(
            "The K-index response structure, field definitions, locations, and "
            "three-hour intervals demonstrated here are based on the "
            "[Australian Bureau of Meteorology Space Weather API]"
            "(https://sws-data.sws.bom.gov.au/api-docs)."
        )

        # State the conservative attribution boundary explicitly.
        st.caption(
            "This project attributes the source but does not claim a specific "
            "licence for observations returned by the API."
        )

    # Attribute the exact NASA dataset used by the OMNI pipeline.
    with st.container(border=True):
        # Name the archive and dataset identifier used in the project.
        st.subheader("NASA SPDF high-resolution OMNI")

        # Provide the official dataset citation and persistent identifier.
        st.markdown(
            "NASA Space Physics Data Facility (SPDF), "
            "**OMNI_HRO2_1MIN**: *OMNI Combined, Definitive 1-minute IMF and "
            "Definitive Plasma Data Time-Shifted to the Nose of the Earth's Bow "
            "Shock, plus Magnetic Indices*. "
            "[Dataset record]"
            "(https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) · "
            "[DOI: 10.48322/mj0k-fq60]"
            "(https://doi.org/10.48322/mj0k-fq60)"
        )

        # Link the archive's own reuse guidance rather than paraphrasing it legally.
        st.caption(
            "NASA SPDF publishes its own "
            "[data-use policy](https://spdf.gsfc.nasa.gov/data_use_policy.html)."
        )

    # Distinguish altered source-derived fixtures from wholly constructed evidence.
    st.subheader("What is demonstrated here?")

    # Summarize provenance without revealing the replacement mechanics.
    st.markdown(
        "- **Modified fixtures:** Numeric K-index and OMNI values were changed "
        "for public presentation. The dashboard does not present them as "
        "historical observations.\n"
        "- **Constructed conflict:** One additional K-index source record was "
        "created specifically to show how the pipeline handles two disagreeing "
        "reports. It was not supplied by the Bureau of Meteorology."
    )

    # Keep the intended use limitation prominent at the end of the page.
    st.warning(
        "The displayed values are educational demonstration data. They must not "
        "be used for historical research, scientific analysis, operational space-"
        "weather decisions, or forecasting."
    )


__all__ = ["render_data_sources_page"]

from pathlib import Path

import streamlit as st

from assessment_page import render_assessment_page
from data_sources_page import render_data_sources_page
from kindex_page import render_kindex_page
from omni_page import render_omni_page


project_root = Path(__file__).resolve().parents[1]
diagram_path = project_root / "specs" / "presentation.svg"

# Configure the browser-tab title and use the full page width for the diagram.
st.set_page_config(
    page_title="Space Weather Data Readiness",
    layout="wide",
)


def render_overview_page() -> None:
    """Render the high-level project architecture."""
    if not diagram_path.is_file():
        # Display a visible error inside the app instead of failing obscurely.
        st.error(f"Architecture diagram not found: {diagram_path}")

        # Stop this Streamlit run because the required presentation asset is absent.
        st.stop()

    # Render the local SVG responsively with accessible alternative text.
    st.image(
        diagram_path,
        width="stretch",
        alt="Space Weather Forecasting System data-readiness architecture",
    )


def render_kindex_page_from_project() -> None:
    """Render the K-index page using this repository's frozen examples."""
    render_kindex_page(project_root=project_root)


def render_omni_page_from_project() -> None:
    """Render the OMNI page using this repository's frozen examples."""
    render_omni_page(project_root=project_root)


def render_assessment_page_from_project() -> None:
    """Render the dataset-assessment page from one persisted bundle."""
    render_assessment_page(project_root=project_root)


# Define the presentation pages without exposing arbitrary file selection.
overview_page = st.Page(
    render_overview_page,
    title="System overview",
    default=True,
)
kindex_page = st.Page(
    render_kindex_page_from_project,
    title="K-index lineage",
)
omni_page = st.Page(
    render_omni_page_from_project,
    title="OMNI lineage",
)
assessment_page = st.Page(
    render_assessment_page_from_project,
    title="Dataset assessment",
)
data_sources_page = st.Page(
    render_data_sources_page,
    title="Data sources & attribution",
)

# Put the compact project navigation in Streamlit's standard sidebar.
navigation = st.navigation(
    [
        overview_page,
        kindex_page,
        omni_page,
        assessment_page,
        data_sources_page,
    ],
    position="sidebar",
)

# Execute only the page selected by the visitor.
navigation.run()

//! Explicit opt-in smoke test for the standalone account/download service.
//! Example: CLF3_COLLECTION_LIVE_PLAN=/path/to/plan.json cargo test
//! --test collection_gui_live -- --ignored --nocapture

#[test]
#[ignore = "queries the public Nexus catalog and downloads one thumbnail; no account is used"]
fn public_catalog_browses_games_search_and_pages_without_credentials() {
    use clf3::collection_app::catalog::{Catalog, Search, Sort};
    let catalog = Catalog::new().unwrap();
    let token = Default::default();
    let games = catalog.games(&token).unwrap();
    assert!(games.iter().any(|g| g.domain_name == "fallout4"));
    assert!(games
        .iter()
        .any(|g| g.domain_name == "skyrimspecialedition"));
    let all = catalog.search(&Search::default(), &token).unwrap();
    assert_eq!(all.nodes.len(), 24);
    assert!(all
        .nodes
        .iter()
        .any(|e| e.game.domain_name != "skyrimspecialedition"));
    let next = catalog
        .search(
            &Search {
                page: 1,
                ..Default::default()
            },
            &token,
        )
        .unwrap();
    assert_ne!(all.nodes[0].url(), next.nodes[0].url());
    let fallout = catalog
        .search(
            &Search {
                game: "fallout4".into(),
                sort: Sort::Updated,
                ..Default::default()
            },
            &token,
        )
        .unwrap();
    assert!(!fallout.nodes.is_empty());
    assert!(fallout
        .nodes
        .iter()
        .all(|e| e.game.domain_name == "fallout4" && e.can_review_installation()));
    let gts = catalog
        .search(
            &Search {
                text: "Gate To Sovngarde".into(),
                game: "skyrimspecialedition".into(),
                ..Default::default()
            },
            &token,
        )
        .unwrap();
    let gts = gts
        .nodes
        .iter()
        .find(|e| e.slug == "qdurkx")
        .expect("GTS should be discoverable, including its adult-content flag");
    assert!(gts.can_review_installation());
    assert!(gts.url().unwrap().contains("/revisions/"));
    let thumbnail = catalog
        .thumbnail(&gts.tile_image.as_ref().unwrap().thumbnail_url, &token)
        .unwrap();
    assert!(thumbnail.width() <= 384 && thumbnail.height() <= 216);
    eprintln!("Public catalog: {} games, {} collections; pagination, Fallout 4, GTS search and thumbnail verified without credentials", games.len(), all.total_count);
}

#[test]
#[ignore = "uses the locally saved Nexus account and a caller-supplied pinned plan"]
fn saved_account_downloads_the_exact_pinned_archive() {
    use clf3::collection::{worker::read_json, CollectionPlan};
    use clf3::collection_app::acquire::{verify_archive, Nexus};
    let path = std::path::PathBuf::from(
        std::env::var_os("CLF3_COLLECTION_LIVE_PLAN").expect("Supply a pinned plan path"),
    );
    let plan: CollectionPlan = read_json(&path, 32 * 1024 * 1024).unwrap();
    assert!(plan.blockers.is_empty());
    let artifact = plan
        .artifacts
        .iter()
        .filter(|a| a.source_type == "nexus" && a.expected_size.is_some_and(|s| s >= 8))
        .min_by_key(|a| a.expected_size.unwrap())
        .expect("Plan needs an exact Nexus artifact");
    let settings = clf3::settings::Settings::load();
    let nexus = Nexus::new(&settings.nexus_api_key).unwrap();
    assert!(
        nexus.premium().unwrap(),
        "This automated live test requires Premium"
    );
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("exact.archive");
    let token = Default::default();
    nexus
        .artifact(artifact, None, &target, &token, &|_| {})
        .unwrap();
    verify_archive(&target, artifact, &token).unwrap();
    eprintln!(
        "Verified pinned Nexus mod {}, file {}, {} bytes",
        artifact.mod_id,
        artifact.file_id,
        target.metadata().unwrap().len()
    );
}

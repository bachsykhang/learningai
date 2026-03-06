document.addEventListener("DOMContentLoaded", () => {
    const levelSelect = document.querySelector("select[data-autosubmit='true']");
    if (levelSelect) {
        levelSelect.addEventListener("change", () => {
            levelSelect.form.submit();
        });
    }

    const revealItems = document.querySelectorAll(".reveal");
    revealItems.forEach((item, index) => {
        item.style.animationDelay = `${index * 0.08}s`;
    });
});

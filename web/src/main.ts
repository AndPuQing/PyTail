document.addEventListener("DOMContentLoaded", () => {
  const form = document.querySelector<HTMLFormElement>("#search");

  form?.addEventListener("submit", (event) => {
    const input = form.querySelector<HTMLInputElement>("input[name=q]");
    const query = input?.value.trim() ?? "";

    if (!query) {
      event.preventDefault();
      input?.focus();
      return;
    }

    const button = form.querySelector<HTMLInputElement | HTMLButtonElement>("[type=submit]");
    if (button) {
      button.disabled = true;
      if (button instanceof HTMLInputElement) {
        button.value = "Opening...";
      } else {
        button.textContent = "Opening...";
      }
    }
  });
});

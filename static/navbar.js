function generateNavbar(navbar_entries) {
  const currentPath = window.location.pathname.split('/').pop();

  return `
    <div class="topnav">
        ${navbar_entries.map(item => `
          <a href="${item.path}" class="${currentPath === item.path ? 'active' : ''}" ${item.style ? `style="${item.style}"` : ''} ${item.title ? `title="${item.title}"` : ''} ${item.id ? `id="${item.id}"` : ''}>
              ${item.icon ? `<i class="${item.icon}"></i>` : ''} ${item.img ? `<div class="topnav-pfp"><img src="${item.img}"></div>` : '' }  ${item.name}
          </a>
        `).join('')}
        <div class="search-container" style="float: left; position: relative;">
            <form action="/action_page.php">
                <input type="text" placeholder="Search.." name="search">
                <button type="submit"><i class="fa fa-search"></i></button>
                <div class="dropdown" style="position: absolute; background-color: white; border: 1px solid #ccc; z-index: 1000; width: 100%; display: none;"></div>
            </form>
        </div>
    </div>
  `;
}

function checkGET(param = '') {
  return new URLSearchParams(window.location.search).has(param);
}

document.addEventListener('DOMContentLoaded', () => {
  const searchInput = document.querySelector('.topnav input[type="text"]');
  const searchContainer = document.querySelector('.topnav .search-container');

  // Create dropdown at the global level
  const dropdown = document.createElement('div');
  dropdown.className = 'dropdown-global';
  dropdown.style.display = 'none';
  dropdown.style.position = 'absolute';
  dropdown.style.zIndex = '1000';
  dropdown.style.backgroundColor = 'white';
  dropdown.style.border = '1px solid #ccc';
  dropdown.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.1)';
  dropdown.style.maxHeight = '200px';
  dropdown.style.overflowY = 'auto';
  dropdown.style.width = `${searchInput.offsetWidth}px`;
  document.body.appendChild(dropdown);

  searchInput.addEventListener('focus', () => {
      const rect = searchInput.getBoundingClientRect();
      dropdown.style.top = `${rect.bottom + window.scrollY}px`;
      dropdown.style.left = `${rect.left + window.scrollX}px`;
      dropdown.style.display = 'block';
  });

  searchInput.addEventListener('input', async () => {
      const query = searchInput.value.trim();
      if (query.length === 0) {
          dropdown.style.display = 'none';
          dropdown.innerHTML = '';
          return;
      }

      try {
          // Fetch search results from the backend
          const response = await fetch(`/search?q=${query}`);
          const results = await response.json();

          dropdown.innerHTML = '';

          if (results.length === 0) {
              dropdown.style.display = 'none';
              return;
          }

          results.forEach(result => {
              const item = document.createElement('div');
              item.textContent = result.name;
              item.style.padding = '8px';
              item.style.cursor = 'pointer';
              item.addEventListener('click', () => {
                  window.location.href = result.url; // Navigate to the selected page
              });
              dropdown.appendChild(item);
          });

          dropdown.style.display = 'block';
      } catch (error) {
          console.error('Error fetching search results:', error);
      }
  });

  document.addEventListener('click', (e) => {
      if (!searchContainer.contains(e.target) && !dropdown.contains(e.target)) {
          dropdown.style.display = 'none';
      }
  });
});
